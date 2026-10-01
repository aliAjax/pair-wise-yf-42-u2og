import hashlib
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, ValidationError
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        if action == "update_parents":
            self._invalidate_approved_pairings(actor, entity_id)
        return updated

    def _invalidate_approved_pairings(self, actor, animal_id):
        """Parentage changed: approved pairings of this animal lose approval.

        They fall back to ``proposed`` and must be approved again against the
        updated pedigree before they can be completed.
        """
        for pairing in self.repository.list_entities(kind="pairing"):
            if pairing["status"] != "approved":
                continue
            data = pairing["data"]
            if data.get("sire_id") != animal_id and data.get("dam_id") != animal_id:
                continue
            next_status, patch = self.rules.validate_transition(
                actor, pairing, "invalidate_approval", {}, self._lookup
            )
            merged = dict(pairing["data"])
            merged.update(patch)
            updated = self.repository.update_entity(
                pairing["id"], pairing["version"], next_status, merged
            )
            self.audit.record(
                pairing["id"],
                actor,
                "invalidate_approval",
                pairing["status"],
                updated["status"],
                {"reason": "parent_update", "animal_id": animal_id},
            )

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    # ----- 同步对账：批量导入外来谱系 -----

    def sync_import(self, actor, payload, batch_id=None):
        """Import a foreign pedigree file (animals + pairings) idempotently.

        Animals are matched by source id: a record whose source id already
        exists is merged into the canonical animal instead of creating a
        duplicate, and the old source id is kept as an alias. Pairings are
        matched by source id and by pair key (sire + dam + cycle), so both
        zoos submitting the same pairing only ever produce one record.

        A failed sync can be retried safely: every record is applied through
        an atomic check-then-create, and a completed batch is returned
        verbatim on retry via its batch id.
        """
        if batch_id:
            prior = self.repository.get_sync_batch(batch_id)
            if prior is not None:
                return prior
        payload = payload or {}
        source = payload.get("source") or payload.get("source_zoo") or ""
        summary = {
            "batch_id": batch_id,
            "source": source,
            "animals": [],
            "pairings": [],
        }
        for item in payload.get("animals", []) or []:
            summary["animals"].append(self._sync_animal(actor, source, item))
        # second pass: parentage may reference animals later in the file
        for item in payload.get("animals", []) or []:
            self._sync_animal_parents(actor, item)
        for item in payload.get("pairings", []) or []:
            summary["pairings"].append(self._sync_pairing(actor, source, item))
        if batch_id:
            self.repository.save_sync_batch(batch_id, actor.user_id, summary)
        return summary

    def _sync_animal(self, actor, source, item):
        if not isinstance(item, dict):
            raise ValidationError("animal record must be a JSON object")
        source_id = item.get("source_id")
        if not source_id:
            raise ValidationError("animal record missing source_id")
        source_id = str(source_id)
        item_source = item.get("source") or source
        data = self._animal_payload(item_source, source_id, item)
        entity, created = self.repository.create_animal_if_absent(
            source_id, str(uuid4()), data, actor.user_id
        )
        if created:
            self.audit.record(
                entity["id"], actor, "create", None, entity["status"],
                {"kind": "animal", "source": item_source, "source_id": source_id, "sync": True},
            )
            return {"source_id": source_id, "id": entity["id"], "merged": False}
        merged = self._merge_animal_data(entity, item_source, source_id, item)
        entity = self.repository.update_entity(
            entity["id"], entity["version"], entity["status"], merged
        )
        self.audit.record(
            entity["id"], actor, "merge", entity["status"], entity["status"],
            {"source": item_source, "source_id": source_id},
        )
        return {"source_id": source_id, "id": entity["id"], "merged": True}

    @staticmethod
    def _animal_payload(source, source_id, item):
        data = {
            key: value
            for key, value in item.items()
            if key not in (
                "source_id", "source", "id", "sources", "aliases",
                "sire_source_id", "dam_source_id",
            )
        }
        data["source_id"] = source_id
        if source:
            data["source"] = source
        data["sources"] = [{"source": source, "source_id": source_id}]
        data.setdefault("aliases", [])
        return data

    @staticmethod
    def _merge_animal_data(entity, source, source_id, item):
        merged = dict(entity["data"])
        for key, value in item.items():
            if key in (
                "source_id", "source", "id", "sources", "aliases",
                "sire_source_id", "dam_source_id",
            ):
                continue
            if key in ("name", "sex") or key not in merged or merged[key] in (None, ""):
                merged[key] = value
        merged.setdefault("sources", [])
        if not any(
            entry.get("source_id") == source_id and (not source or entry.get("source") == source)
            for entry in merged["sources"]
        ):
            merged["sources"].append({"source": source, "source_id": source_id})
        merged.setdefault("aliases", [])
        primary = merged.get("source_id")
        if source_id != primary and source_id not in merged["aliases"]:
            merged["aliases"].append(source_id)
        return merged

    def _sync_animal_parents(self, actor, item):
        source_id = item.get("source_id")
        if not source_id:
            return
        entity = self.repository.find_animal_by_source(str(source_id))
        if not entity:
            return
        sire_ref = item.get("sire_id") or item.get("sire_source_id")
        dam_ref = item.get("dam_id") or item.get("dam_source_id")
        if not sire_ref and not dam_ref:
            return
        sire = self._resolve_animal(sire_ref)
        dam = self._resolve_animal(dam_ref)
        if sire_ref and not sire:
            raise ValidationError(
                "animal %s references unknown sire %s" % (source_id, sire_ref)
            )
        if dam_ref and not dam:
            raise ValidationError(
                "animal %s references unknown dam %s" % (source_id, dam_ref)
            )
        data = {}
        if sire and sire["id"] != entity["data"].get("sire_id"):
            data["sire_id"] = sire["id"]
        if dam and dam["id"] != entity["data"].get("dam_id"):
            data["dam_id"] = dam["id"]
        if not data:
            return
        self.transition(
            actor, entity["id"], "update_parents", data,
            expected_version=entity["version"],
        )

    def _resolve_animal(self, ref):
        if not ref:
            return None
        ref = str(ref)
        entity = self.repository.find_animal_by_source(ref)
        if entity:
            return entity
        entity = self.repository.get_entity(ref)
        if entity and entity["kind"] == "animal":
            return entity
        return None

    def _sync_pairing(self, actor, source, item):
        if not isinstance(item, dict):
            raise ValidationError("pairing record must be a JSON object")
        source_id = item.get("source_id")
        if not source_id:
            raise ValidationError("pairing record missing source_id")
        source_id = str(source_id)
        item_source = item.get("source") or source
        sire = self._resolve_animal(item.get("sire_id") or item.get("sire_source_id"))
        dam = self._resolve_animal(item.get("dam_id") or item.get("dam_source_id"))
        if not sire or not dam:
            raise ValidationError(
                "pairing %s references unknown sire/dam" % source_id
            )
        season = str(item.get("season") or item.get("cycle") or item.get("year") or "default")
        pair_key = hashlib.sha1(
            ("%s|%s|%s" % (sire["id"], dam["id"], season)).encode("utf-8")
        ).hexdigest()
        data = {
            key: value
            for key, value in item.items()
            if key not in (
                "source_id", "source", "id", "sire_source_id", "dam_source_id",
                "sire_id", "dam_id", "season", "cycle", "year",
            )
        }
        data["sire_id"] = sire["id"]
        data["dam_id"] = dam["id"]
        data["season"] = season
        data["source_id"] = source_id
        if item_source:
            data["source"] = item_source
        data.setdefault("proposed_by", actor.user_id)
        entity, created = self.repository.create_pairing_if_absent(
            source_id, pair_key, str(uuid4()), data, actor.user_id
        )
        if created:
            self.audit.record(
                entity["id"], actor, "create", None, "proposed",
                {"kind": "pairing", "source": item_source, "source_id": source_id, "sync": True},
            )
            return {"source_id": source_id, "id": entity["id"], "deduplicated": False}
        self.repository.bind_pairing_source(source_id, entity["id"])
        self.audit.record(
            entity["id"], actor, "pairing_deduplicated", "proposed", entity["status"],
            {"source": item_source, "source_id": source_id},
        )
        return {"source_id": source_id, "id": entity["id"], "deduplicated": True}

    def get_sync_batch(self, batch_id):
        return self.repository.get_sync_batch(batch_id)
