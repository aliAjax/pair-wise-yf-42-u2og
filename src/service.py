from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError
from .rules import RuleEngine
from .sync import SyncCoordinator


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)
        self.sync = SyncCoordinator(repository, self.rules)

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
                entity = self._resolve(existing)
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
        target = self._resolve_id(entity_id)
        entity = self.repository.get_entity(target)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        payload = dict(data or {})
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, payload, self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        if entity["kind"] == "pairing":
            return self._transition_pairing(
                actor, entity, action, next_status, merged, expected, patch
            )
        updated = self.repository.update_entity(target, expected, next_status, merged)
        self.audit.record(
            target, actor, action, entity["status"], updated["status"], {"patch": patch}
        )
        return updated

    def _transition_pairing(self, actor, entity, action, next_status, merged,
                            expected, patch):
        pairing_id = entity["id"]
        sire_id = merged.get("sire_id")
        dam_id = merged.get("dam_id")
        with self.repository.transaction() as connection:
            current = self.repository.conn_get_entity(connection, pairing_id)
            if current["version"] != expected:
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected, current["version"])
                )
            if action == "approve" and sire_id and dam_id:
                slot = self.repository.conn_get_slot(connection, sire_id, dam_id)
                if slot and slot["pairing_id"] != pairing_id:
                    raise ConflictError(
                        "pairing slot already taken by %s" % slot["pairing_id"]
                    )
            if action in ("reject", "complete") and sire_id and dam_id:
                slot = self.repository.conn_get_slot(connection, sire_id, dam_id)
                if slot and slot["pairing_id"] == pairing_id:
                    self.repository.conn_release_slot(connection, sire_id, dam_id)
            self.repository.conn_upsert_entity(
                connection, pairing_id, "pairing", next_status, merged,
                actor.user_id, bump_version=True,
            )
            if action == "approve" and sire_id and dam_id:
                self.repository.conn_occupy_slot(
                    connection, sire_id, dam_id, pairing_id, next_status
                )
            self.repository.conn_append_audit(
                connection, pairing_id, actor.user_id, actor.role, action,
                entity["status"], next_status, {"patch": patch},
            )
        return self.repository.get_entity(pairing_id)

    def _resolve_id(self, entity_id):
        seen = set()
        current = entity_id
        while current and current not in seen:
            seen.add(current)
            redirected = self.repository.get_redirect(current)
            if not redirected or redirected == current:
                break
            current = redirected
        return current

    def _resolve(self, entity_id):
        return self.repository.get_entity(self._resolve_id(entity_id))

    def get(self, entity_id):
        entity = self._resolve(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    def import_sync_batch(self, actor, sync_key, source_system, animals=None,
                          pairings=None):
        return self.sync.import_batch(
            actor, sync_key, source_system,
            animals=animals, pairings=pairings,
        )

    def sync_status(self, sync_key):
        result = self.sync.get_status(sync_key)
        if not result:
            raise NotFoundError("sync batch not found: " + sync_key)
        return result

    def source_refs(self, canonical_id=None):
        return self.repository.list_source_refs(canonical_id=canonical_id)

    def pairing_slot(self, sire_id, dam_id):
        sire = self._resolve_id(sire_id)
        dam = self._resolve_id(dam_id)
        slot = self.repository.get_slot(sire, dam)
        if not slot:
            raise NotFoundError("no pairing occupies that slot")
        return slot
