"""Two-zoo studbook reconciliation.

A *sync batch* imports animals and pairings from an outside institution.
Its guarantees are:

* Animals that arrive under duplicate source ids (or that match an animal
  already on file by studbook id, or by same name) are merged into one
  canonical animal.  Every old id is preserved in ``source_refs`` and every
  local entity id that disappears is recorded in ``entity_redirects``.
* A sire/dam pairing slot can only be claimed once, so two zoos submitting
  the same pairing concurrently end up with exactly one effective pairing.
* Whenever an animal's parents change, any approved pairing built on the
  old lineage falls back to ``needs_confirmation`` and must be re-approved.
* Each item is checkpointed inside its own transaction.  Replaying a batch
  after a failure resumes from the checkpoint and can never create extra
  animals or claim extra slots.
"""

import json

from .domain import (
    ConflictError,
    PermissionDenied,
    ValidationError,
)

ANIMAL_FIELDS = ("name", "sex", "studbook_id", "sire_ref", "dam_ref",
                 "sire_id", "dam_id", "birth_date")
PAIRING_FIELDS = ("proposed_by", "planned_date", "notes")
SYNC_ROLES = ("admin", "coordinator", "registrar")

ACTIVE_PAIRING_STATUSES = ("proposed", "approved", "needs_confirmation")


def _norm_name(value):
    return " ".join(str(value or "").strip().lower().split())


class _DSU:
    def __init__(self):
        self.parent = {}

    def add(self, item):
        self.parent.setdefault(item, item)

    def find(self, item):
        self.add(item)
        root = item
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[item] != root:
            self.parent[item], item = root, self.parent[item]
        return root

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


class SyncCoordinator:
    def __init__(self, repository, rules):
        self.repository = repository
        self.rules = rules
        # Test/injection hook invoked right before an item is applied.
        self.before_item = None

    # ------------------------------------------------------------------
    # Reference resolution

    def _resolve_ref(self, ref, source_system, local_index, batch_index=None,
                     kind="animal"):
        if ref is None or ref == "":
            return None
        if not isinstance(ref, str):
            raise ValidationError("parent reference must be a string: %r" % (ref,))
        if batch_index is not None and ref in batch_index:
            return batch_index[ref]
        if ref in local_index:
            return local_index[ref]
        if ":" in ref:
            system, source_id = ref.split(":", 1)
            source = self.repository.get_source_ref(system.strip(), source_id.strip())
            if source:
                return source["canonical_id"]
        else:
            source = self.repository.get_source_ref(source_system, ref)
            if source:
                return source["canonical_id"]
            # The other zoo may reuse a source number it received from the
            # originating institution; resolve when the number is unique
            # across institutions.
            foreign = self.repository.find_source_ref_any_system(ref, kind=kind)
            canonicals = {row["canonical_id"] for row in foreign}
            if len(canonicals) == 1:
                return next(iter(canonicals))
        existing = self.repository.get_entity(ref)
        if existing:
            return existing["id"]
        return None

    # ------------------------------------------------------------------
    # Batch entry point

    def import_batch(self, actor, sync_key, source_system, animals=None,
                     pairings=None):
        if actor.role not in SYNC_ROLES:
            raise PermissionDenied("role %s may not submit a sync batch" % actor.role)
        animals = animals or []
        pairings = pairings or []
        if not isinstance(animals, list) or not isinstance(pairings, list):
            raise ValidationError("animals and pairings must be arrays")
        self._validate_shapes(animals, pairings)

        prior = self.repository.register_batch(sync_key, source_system)
        if prior["status"] == "completed":
            return self._result(sync_key, resumed=False, already_done=True)

        outcome = {"failed": False, "warnings": []}
        animal_canonical = self._process_animals(
            actor, sync_key, source_system, animals, outcome
        )
        self._process_pairings(
            actor, sync_key, source_system, pairings, animal_canonical, outcome
        )
        self.repository.set_batch_status(
            sync_key, "failed" if outcome["failed"] else "completed"
        )
        return self._result(sync_key, resumed=True,
                            already_done=False, warnings=outcome["warnings"])

    def _validate_shapes(self, animals, pairings):
        seen = set()
        for index, record in enumerate(animals):
            if not isinstance(record, dict):
                raise ValidationError("animal %d must be an object" % index)
            source_id = record.get("source_id")
            if not source_id:
                raise ValidationError("animal %d is missing source_id" % index)
            if record.get("sex", "unknown") not in ("male", "female", "unknown"):
                raise ValidationError("animal %s has invalid sex" % source_id)
            item_ref = str(source_id)
            if item_ref in seen:
                raise ValidationError("duplicate source_id in batch: " + item_ref)
            seen.add(item_ref)
            for field in ("sire_ref", "dam_ref", "sire_id", "dam_id"):
                value = record.get(field)
                if value is not None and not isinstance(value, str):
                    raise ValidationError(
                        "%s of animal %s must be a string" % (field, source_id)
                    )
        seen = set()
        for index, record in enumerate(pairings):
            if not isinstance(record, dict):
                raise ValidationError("pairing %d must be an object" % index)
            key = record.get("source_id") or self._pairing_key(record)
            if key in seen:
                raise ValidationError("duplicate pairing in batch: " + str(key))
            seen.add(key)
            sire = record.get("sire_ref", record.get("sire_id"))
            dam = record.get("dam_ref", record.get("dam_id"))
            if not sire or not dam:
                raise ValidationError(
                    "pairing %s requires sire and dam references" % key
                )

    @staticmethod
    def _pairing_key(record):
        sire = record.get("sire_ref", record.get("sire_id", ""))
        dam = record.get("dam_ref", record.get("dam_id", ""))
        return "pair:%s|%s" % (sire, dam)

    def _item_ref(self, record, pairings=False):
        if pairings:
            return str(record.get("source_id") or self._pairing_key(record))
        return str(record["source_id"])

    # ------------------------------------------------------------------
    # Animals

    def _load_animal_indexes(self):
        local = {}
        by_studbook = {}
        by_name = {}
        for entity in self.repository.list_entities(kind="animal"):
            if entity["status"] == "merged":
                continue
            data = entity["data"]
            local[entity["id"]] = entity["id"]
            studbook = data.get("studbook_id")
            if studbook:
                by_studbook.setdefault(str(studbook), []).append(entity)
            name = _norm_name(data.get("name"))
            if name:
                by_name.setdefault(name, []).append(entity)
        return local, by_studbook, by_name

    def _process_animals(self, actor, sync_key, source_system, animals, outcome):
        local_index, by_studbook, by_name = self._load_animal_indexes()
        dsu = _DSU()
        bound = {}  # batch item -> existing canonical entity

        def bind(item_ref, entity):
            existing = bound.get(item_ref)
            if existing and existing["id"] != entity["id"]:
                dsu.union(
                    self._key_of(bound, existing["id"]),
                    self._key_of(bound, entity["id"]),
                )
            bound[item_ref] = entity
            dsu.union(item_ref, "entity:" + entity["id"])

        for record in animals:
            item_ref = self._item_ref(record)
            dsu.add(item_ref)
            source = self.repository.get_source_ref(source_system, item_ref)
            if source:
                target = self.repository.get_entity(source["canonical_id"])
                if target:
                    bind(item_ref, target)
            local_id = record.get("id")
            if local_id:
                target = self.repository.get_entity(str(local_id))
                if target and target["status"] != "merged":
                    bind(item_ref, target)
                local_index[str(local_id)] = str(local_id)
            studbook = record.get("studbook_id")
            if studbook:
                matches = by_studbook.get(str(studbook), [])
                for match in matches:
                    bind(item_ref, match)
            for alias in record.get("alias_refs", []) or []:
                resolved = self._resolve_ref(alias, source_system, local_index)
                if resolved:
                    target = self.repository.get_entity(resolved)
                    if target and target["status"] != "merged":
                        bind(item_ref, target)

        name_groups = {}
        for record in animals:
            name = _norm_name(record.get("name"))
            if name:
                name_groups.setdefault(name, []).append(self._item_ref(record))
        for name, members in name_groups.items():
            for match in by_name.get(name, []):
                for member in members:
                    bind(member, match)
            root = members[0]
            for member in members[1:]:
                dsu.union(root, member)

        components = {}
        for record in animals:
            components.setdefault(dsu.find(self._item_ref(record)), []).append(record)

        canonical_of = {}
        for root in sorted(components):
            records = components[root]
            item_refs = [self._item_ref(r) for r in records]
            existing = [bound[ref] for ref in item_refs if ref in bound]
            canonical = self._merge_component(
                actor, sync_key, source_system, records, existing, outcome
            )
            for ref in item_refs:
                canonical_of[ref] = canonical

        # Parentage is applied only after every component has merged so that
        # in-batch parent references resolve to canonical animals.  A lineage
        # change invalidates approvals already recorded against the animal.
        self._apply_parentage(
            actor, source_system, animals, canonical_of, outcome
        )
        return canonical_of

    @staticmethod
    def _key_of(bound, entity_id):
        for key, entity in bound.items():
            if entity["id"] == entity_id:
                return key
        return "entity:" + entity_id

    def _merge_component(self, actor, sync_key, source_system, records,
                         existing, outcome):
        ordered = sorted(records, key=lambda r: (
            0 if self.repository.get_source_ref(source_system, self._item_ref(r))
            else 1,
            self._item_ref(r),
        ))
        sexes = {r.get("sex") for r in records if r.get("sex") and r.get("sex") != "unknown"}
        if len(sexes) > 1:
            self._fail_items(
                sync_key, [self._item_ref(r) for r in records],
                "sex conflict among duplicate animals: %s" % sorted(sexes),
            )
            outcome["failed"] = True
            return None

        canonical_entity = None
        if existing:
            canonical_entity = sorted(
                existing, key=lambda e: (e["created_at"], e["id"])
            )[0]
        canonical_id = canonical_entity["id"] if canonical_entity else str(
            ordered[0].get("id") or self._new_id(ordered[0], source_system)
        )

        merged_data = dict(canonical_entity["data"]) if canonical_entity else {}
        for record in ordered:
            for field in ANIMAL_FIELDS:
                value = record.get(field)
                if value not in (None, "", []):
                    merged_data[field] = value
        merged_data.pop("sire_ref", None)
        merged_data.pop("dam_ref", None)

        with self.repository.transaction() as connection:
            from_status = canonical_entity["status"] if canonical_entity else None
            if canonical_entity:
                self.repository.conn_upsert_entity(
                    connection, canonical_id, "animal",
                    canonical_entity["status"], merged_data,
                    actor.user_id, bump_version=True,
                )
            else:
                self.repository.conn_upsert_entity(
                    connection, canonical_id, "animal", "active",
                    merged_data, actor.user_id,
                )
            self.repository.conn_append_audit(
                connection, canonical_id, actor.user_id, actor.role,
                "sync_merge" if canonical_entity else "sync_import",
                from_status, canonical_entity["status"] if canonical_entity else "active",
                {"source_system": source_system,
                 "sources": [self._item_ref(r) for r in ordered]},
            )
            merged_ids = {e["id"] for e in existing if e["id"] != canonical_id}
            for record in ordered:
                item_ref = self._item_ref(record)
                local_id = record.get("id")
                if local_id and str(local_id) != canonical_id:
                    merged_ids.add(str(local_id))
                self.repository.conn_save_source_ref(
                    connection, source_system, item_ref, "animal", canonical_id
                )
                self.repository.conn_upsert_item(
                    connection, sync_key, item_ref, "animal", "completed",
                    canonical_id, {"merged_from": sorted(merged_ids)}
                    if merged_ids else {},
                )
            for old_id in sorted(merged_ids):
                old_entity = self.repository.conn_get_entity(connection, old_id)
                if not old_entity:
                    # A foreign record carried a local id that was never seen
                    # here.  Materialise a merged tombstone so that the old
                    # number stays resolvable and its destination is known.
                    self.repository.conn_upsert_entity(
                        connection, old_id, "animal", "merged",
                        dict(merged_data), actor.user_id,
                    )
                    old_entity = self.repository.conn_get_entity(connection, old_id)
                else:
                    self.repository.conn_upsert_entity(
                        connection, old_id, "animal", "merged",
                        dict(old_entity["data"]), actor.user_id,
                    )
                self._rewrite_refs(connection, old_id, canonical_id)
                self.repository.conn_append_audit(
                    connection, old_id, actor.user_id, actor.role,
                    "merged_into", old_entity["status"], "merged",
                    {"canonical_id": canonical_id},
                )
                self.repository.conn_save_redirect(
                    connection, old_id, canonical_id, "duplicate animal merge"
                )
                self.repository.conn_save_source_ref(
                    connection, source_system, old_id, "animal", canonical_id,
                    merged_into=canonical_id,
                )
        return canonical_id

    @staticmethod
    def _new_id(record, source_system):
        return "animal-%s-%s" % (source_system, record["source_id"])

    def _rewrite_refs(self, connection, old_id, canonical_id):
        reference_fields = (
            ("animal", ("sire_id", "dam_id")),
            ("transfer", ("animal_id",)),
        )
        for kind, fields in reference_fields:
            rows = connection.execute(
                "SELECT * FROM entities WHERE kind = ? AND id != ?",
                (kind, canonical_id),
            ).fetchall()
            for row in rows:
                data = json.loads(row["data"])
                changed = False
                for field in fields:
                    if data.get(field) == old_id:
                        data[field] = canonical_id
                        changed = True
                if changed:
                    connection.execute(
                        "UPDATE entities SET data = ? WHERE id = ?",
                        (json.dumps(data, ensure_ascii=False, sort_keys=True),
                         row["id"]),
                    )
        # The merged-away animal is the same individual as the canonical
        # one, never its own parent; drop self-referential lineage pointers.
        canonical_row = connection.execute(
            "SELECT * FROM entities WHERE id = ?", (canonical_id,)
        ).fetchone()
        if canonical_row:
            data = json.loads(canonical_row["data"])
            changed = False
            if data.get("sire_id") == old_id:
                data.pop("sire_id", None)
                changed = True
            if data.get("dam_id") == old_id:
                data.pop("dam_id", None)
                changed = True
            if changed:
                connection.execute(
                    "UPDATE entities SET data = ? WHERE id = ?",
                    (json.dumps(data, ensure_ascii=False, sort_keys=True),
                     canonical_id),
                )
        rows = connection.execute(
            "SELECT * FROM entities WHERE kind = 'pairing'"
        ).fetchall()
        moved_slots = []
        for row in rows:
            data = json.loads(row["data"])
            changed = False
            for field in ("sire_id", "dam_id"):
                if data.get(field) == old_id:
                    data[field] = canonical_id
                    changed = True
            if changed:
                connection.execute(
                    "UPDATE entities SET data = ? WHERE id = ?",
                    (json.dumps(data, ensure_ascii=False, sort_keys=True),
                     row["id"]),
                )
                moved_slots.append(
                    (row["id"], row["status"], data["sire_id"], data["dam_id"])
                )
        # Relocate claimed slots so two merged animals do not leave the old
        # sire/dam key occupied behind them.  Before merging there can be
        # slots keyed by both animals; the canonical key wins.
        for pairing_id, status, new_sire, new_dam in moved_slots:
            self.repository.conn_release_slot(connection, old_id, new_dam)
            self.repository.conn_release_slot(connection, new_sire, old_id)
            holder = self.repository.conn_get_slot(connection, new_sire, new_dam)
            if holder and holder["pairing_id"] != pairing_id:
                continue
            self.repository.conn_release_slot(connection, new_sire, new_dam)
            self.repository.conn_occupy_slot(
                connection, new_sire, new_dam, pairing_id, status
            )

    def _fail_items(self, sync_key, item_refs, message):
        for item_ref in item_refs:
            self._save_item(sync_key, item_ref, "animal", "failed", None,
                            {"error": message})

    def _save_item(self, sync_key, item_ref, kind, status, canonical_id, detail):
        with self.repository.transaction() as connection:
            self.repository.conn_upsert_item(
                connection, sync_key, item_ref, kind, status,
                canonical_id, detail,
            )

    # ------------------------------------------------------------------
    # Parentage updates

    def _apply_parentage(self, actor, source_system, records, canonical_of,
                         outcome):
        local_index, _, _ = self._load_animal_indexes()
        batch_index = dict(canonical_of)
        for record in records:
            item_ref = self._item_ref(record)
            canonical_id = canonical_of.get(item_ref)
            if not canonical_id:
                continue
            new_sire = self._resolve_ref(
                record.get("sire_ref", record.get("sire_id")),
                source_system, local_index, batch_index,
            )
            new_dam = self._resolve_ref(
                record.get("dam_ref", record.get("dam_id")),
                source_system, local_index, batch_index,
            )
            if (record.get("sire_ref") and not new_sire) or \
               (record.get("dam_ref") and not new_dam):
                outcome["warnings"].append(
                    "animal %s references unknown parent; parentage skipped"
                    % item_ref
                )
                continue
            entity = self.repository.get_entity(canonical_id)
            data = dict(entity["data"])
            old_sire, old_dam = data.get("sire_id"), data.get("dam_id")
            if new_sire:
                data["sire_id"] = new_sire
            if new_dam:
                data["dam_id"] = new_dam
            lineage_changed = (
                (new_sire is not None and new_sire != old_sire)
                or (new_dam is not None and new_dam != old_dam)
            )
            if not lineage_changed:
                continue
            invalidated = self._invalidate_for_lineage(
                actor, canonical_id, old_sire, old_dam, data, source_system
            )
            if invalidated:
                outcome["warnings"].append(
                    "lineage update for %s reset %d approved pairing(s) "
                    "for reconfirmation" % (canonical_id, len(invalidated))
                )

    def _invalidate_for_lineage(self, actor, animal_id, old_sire, old_dam,
                                new_data, source_system):
        invalidated = []
        with self.repository.transaction() as connection:
            entity = self.repository.conn_get_entity(connection, animal_id)
            self.repository.conn_upsert_entity(
                connection, animal_id, "animal", entity["status"],
                new_data, actor.user_id, bump_version=True,
            )
            self.repository.conn_append_audit(
                connection, animal_id, actor.user_id, actor.role,
                "lineage_update", entity["status"], entity["status"],
                {"old_sire_id": old_sire, "old_dam_id": old_dam,
                 "new_sire_id": new_data.get("sire_id"),
                 "new_dam_id": new_data.get("dam_id"),
                 "source_system": source_system},
            )
            for pairing in self.repository.conn_active_pairings(connection):
                data = pairing["data"]
                if pairing["status"] != "approved":
                    continue
                if data.get("sire_id") not in (animal_id, old_sire) and \
                   data.get("dam_id") not in (animal_id, old_dam):
                    continue
                history = list(data.get("approval_history") or [])
                history.append({
                    "approved_by": data.get("approved_by"),
                    "approvals": data.get("approvals"),
                    "reason": "lineage changed for %s" % animal_id,
                })
                data["approval_history"] = history
                data["approvals"] = []
                data.pop("approved_by", None)
                data["needs_reconfirmation_reason"] = (
                    "lineage changed for %s" % animal_id
                )
                self.repository.conn_upsert_entity(
                    connection, pairing["id"], "pairing",
                    "needs_confirmation", data, actor.user_id,
                    bump_version=True,
                )
                self.repository.conn_occupy_slot(
                    connection, data["sire_id"], data["dam_id"],
                    pairing["id"], "needs_confirmation",
                )
                self.repository.conn_append_audit(
                    connection, pairing["id"], actor.user_id, actor.role,
                    "request_reconfirmation", "approved",
                    "needs_confirmation",
                    {"reason": "lineage changed", "animal_id": animal_id},
                )
                invalidated.append(pairing["id"])
        return invalidated

    # ------------------------------------------------------------------
    # Pairings

    def _process_pairings(self, actor, sync_key, source_system, pairings,
                          animal_canonical, outcome):
        local_index, _, _ = self._load_animal_indexes()
        batch_index = dict(animal_canonical)

        for record in pairings:
            item_ref = self._item_ref(record, pairings=True)
            checkpoint = self.repository.get_item(sync_key, item_ref)
            if checkpoint and checkpoint["status"] == "completed":
                continue
            if self.before_item is not None:
                self.before_item("pairing", item_ref, record)
            sire_id = self._resolve_ref(
                record.get("sire_ref", record.get("sire_id")),
                source_system, local_index, batch_index,
            )
            dam_id = self._resolve_ref(
                record.get("dam_ref", record.get("dam_id")),
                source_system, local_index, batch_index,
            )
            if not sire_id or not dam_id:
                self._save_item(sync_key, item_ref, "pairing", "failed", None,
                                {"error": "sire/dam do not resolve to animals"})
                outcome["failed"] = True
                continue
            sire, dam = self.repository.get_entity(sire_id), self.repository.get_entity(dam_id)
            if not sire or not dam or sire["status"] != "active" or dam["status"] != "active":
                self._save_item(sync_key, item_ref, "pairing", "failed", None,
                                {"error": "pairing animals must be active"})
                outcome["failed"] = True
                continue
            try:
                pairing_id, status, note = self._upsert_pairing(
                    actor, sync_key, item_ref, source_system, record,
                    sire_id, dam_id,
                )
            except (ValidationError, ConflictError) as exc:
                self._save_item(sync_key, item_ref, "pairing", "failed", None,
                                {"error": str(exc)})
                outcome["failed"] = True
                continue
            self._save_item(sync_key, item_ref, "pairing", "completed",
                            pairing_id, {"status": status, "note": note})

    def _upsert_pairing(self, actor, sync_key, item_ref, source_system, record,
                        sire_id, dam_id):
        existing_source = None
        if record.get("source_id"):
            existing_source = self.repository.get_source_ref(
                source_system, str(record["source_id"])
            )
            if not existing_source:
                # The same pairing record is forwarded by the other zoo
                # carrying the originator's source number.
                foreign = self.repository.find_source_ref_any_system(
                    str(record["source_id"]), kind="pairing"
                )
                canonicals = {row["canonical_id"] for row in foreign}
                if len(canonicals) == 1:
                    existing_source = foreign[0]

        with self.repository.transaction() as connection:
            slot = self.repository.conn_get_slot(connection, sire_id, dam_id)
            pairing = None
            if existing_source:
                pairing = self.repository.conn_get_entity(
                    connection, existing_source["canonical_id"]
                )
            if pairing is None and slot and slot["status"] in ACTIVE_PAIRING_STATUSES:
                held = self.repository.conn_get_entity(
                    connection, slot["pairing_id"]
                )
                if held and held["status"] in ACTIVE_PAIRING_STATUSES:
                    pairing = held

            if pairing is not None:
                if pairing["status"] in ("rejected", "completed"):
                    self.repository.conn_save_source_ref(
                        connection, source_system, item_ref, "pairing",
                        pairing["id"],
                    )
                    return pairing["id"], pairing["status"], \
                        "existing %s pairing not reopened" % pairing["status"]
                data = dict(pairing["data"])
                data["sire_id"] = sire_id
                data["dam_id"] = dam_id
                note = "matched existing pairing"
                next_status = pairing["status"]
                if pairing["status"] == "approved" and (
                    pairing["data"].get("sire_id") != sire_id
                    or pairing["data"].get("dam_id") != dam_id
                ):
                    history = list(data.get("approval_history") or [])
                    history.append({
                        "approved_by": data.get("approved_by"),
                        "approvals": data.get("approvals"),
                        "reason": "parents changed on sync",
                    })
                    data["approval_history"] = history
                    data["approvals"] = []
                    data.pop("approved_by", None)
                    data["needs_reconfirmation_reason"] = "parents changed on sync"
                    next_status = "needs_confirmation"
                    self.repository.conn_upsert_entity(
                        connection, pairing["id"], "pairing",
                        next_status, data, actor.user_id,
                        bump_version=True,
                    )
                    self.repository.conn_occupy_slot(
                        connection, sire_id, dam_id, pairing["id"],
                        next_status,
                    )
                    self.repository.conn_append_audit(
                        connection, pairing["id"], actor.user_id, actor.role,
                        "request_reconfirmation", "approved",
                        next_status, {"reason": "parents changed"},
                    )
                    note = "parents changed; approval withdrawn"
                else:
                    self.repository.conn_upsert_entity(
                        connection, pairing["id"], "pairing",
                        pairing["status"], data, actor.user_id,
                    )
                self.repository.conn_save_source_ref(
                    connection, source_system, item_ref, "pairing", pairing["id"]
                )
                if record.get("source_id") and str(record["source_id"]) != item_ref:
                    self.repository.conn_save_source_ref(
                        connection, source_system, str(record["source_id"]),
                        "pairing", pairing["id"],
                    )
                return pairing["id"], next_status, note

            return self._create_pairing(
                connection, actor, source_system, item_ref, record,
                sire_id, dam_id,
            )

    def _create_pairing(self, connection, actor, source_system, item_ref,
                        record, sire_id, dam_id):
        sire = self.repository.conn_get_entity(connection, sire_id)
        dam = self.repository.conn_get_entity(connection, dam_id)
        want_approved = record.get("status") == "approved"
        data = {
            "proposed_by": record.get("proposed_by", source_system + ":coordinator"),
            "sire_id": sire_id,
            "dam_id": dam_id,
        }
        for field in PAIRING_FIELDS:
            if record.get(field) not in (None, ""):
                data[field] = record[field]
        if want_approved:
            from .rules import inbreeding_coefficient
            coefficient = inbreeding_coefficient(sire["data"], dam["data"])
            if coefficient > 0.125:
                raise ValidationError("pairing exceeds inbreeding threshold")
            data["approvals"] = record.get("approvals") or [actor.user_id]
            data["approved_by"] = record.get("approved_by", actor.user_id)
            status = "approved"
        else:
            status = "proposed"
        pairing_id = str(record.get("id") or "pairing-%s-%s" % (
            source_system, item_ref.replace(":", "-").replace("|", "-")
        ))
        if self.repository.conn_get_entity(connection, pairing_id):
            raise ConflictError("pairing id already exists: " + pairing_id)
        self.repository.conn_upsert_entity(
            connection, pairing_id, "pairing", status, data, actor.user_id,
        )
        self.repository.conn_occupy_slot(
            connection, sire_id, dam_id, pairing_id, status
        )
        self.repository.conn_append_audit(
            connection, pairing_id, actor.user_id, actor.role,
            "sync_import_pairing", None, status,
            {"source_system": source_system, "source_id": item_ref},
        )
        self.repository.conn_save_source_ref(
            connection, source_system, item_ref, "pairing", pairing_id
        )
        return pairing_id, status, "created"

    # ------------------------------------------------------------------
    # Result

    def _result(self, sync_key, resumed, already_done, warnings=None):
        batch = self.repository.get_batch(sync_key)
        items = self.repository.list_items(sync_key)
        return {
            "sync_key": sync_key,
            "status": batch["status"],
            "resumed": resumed,
            "already_complete": already_done,
            "warnings": warnings or [],
            "items": [
                {
                    "ref": item["item_ref"],
                    "kind": item["kind"],
                    "status": item["status"],
                    "canonical_id": item["canonical_id"],
                    "detail": item["detail"],
                }
                for item in items
            ],
        }

    def get_status(self, sync_key):
        batch = self.repository.get_batch(sync_key)
        if not batch:
            return None
        return self._result(sync_key, resumed=False, already_done=False)
