from datetime import datetime, timedelta

from .domain import (
    ConflictError,
    InvalidTransition,
    PermissionDenied,
    ValidationError,
)


def _validate_animal(actor, data, lookup):
    if data.get("sex") not in ("male", "female", "unknown"):
        raise ValidationError("sex must be male, female or unknown")


def inbreeding_coefficient(sire, dam):
    if not sire or not dam:
        return 1.0
    sire_id = sire.get("id")
    dam_id = dam.get("id")
    if sire_id is None or dam_id is None:
        return 0.0
    if sire_id == dam_id:
        return 0.5
    if sire.get("sire_id") == dam_id or dam.get("sire_id") == sire_id:
        return 0.25
    return 0.0


def _validate_pairing(actor, entity, data, lookup):
    sire = _find_one(lookup, "animal", "id", data.get("sire_id"))
    dam = _find_one(lookup, "animal", "id", data.get("dam_id"))
    if not sire or not dam:
        raise ValidationError("pairing requires two existing animals")
    if sire["status"] != "active" or dam["status"] != "active":
        raise ValidationError("pairing animals must be active")
    if inbreeding_coefficient(sire["data"], dam["data"]) > 0.125:
        raise ValidationError("pairing exceeds inbreeding threshold")
    return {"approved_by": actor.user_id, "approval_invalidated": False}


def _validate_invalidate_approval(actor, entity, data, lookup):
    return {"approval_invalidated": True}


def _parent_data(lookup, parent_id):
    if not parent_id:
        return None
    rows = lookup("animal", "id", parent_id) or []
    return rows[0]["data"] if rows else None


def _validate_parents_update(actor, entity, data, lookup):
    animal_id = entity["id"]
    sire_id = data.get("sire_id")
    dam_id = data.get("dam_id")
    if not sire_id and not dam_id:
        raise ValidationError("at least one of sire_id or dam_id is required")
    patch = {}
    sire = None
    dam = None
    if sire_id:
        sire = _find_one(lookup, "animal", "id", sire_id)
        if not sire:
            raise ValidationError("sire not found: " + str(sire_id))
        if sire["id"] == animal_id:
            raise ValidationError("animal cannot be its own sire")
        if sire["status"] != "active":
            raise ValidationError("sire must be active")
        sire_data = sire["data"]
        if sire_data.get("sire_id") == animal_id or sire_data.get("dam_id") == animal_id:
            raise ValidationError("lineal inbreeding: animal is an ancestor of its sire")
        patch["sire_id"] = sire_id
    if dam_id:
        dam = _find_one(lookup, "animal", "id", dam_id)
        if not dam:
            raise ValidationError("dam not found: " + str(dam_id))
        if dam["id"] == animal_id:
            raise ValidationError("animal cannot be its own dam")
        if dam["status"] != "active":
            raise ValidationError("dam must be active")
        dam_data = dam["data"]
        if dam_data.get("sire_id") == animal_id or dam_data.get("dam_id") == animal_id:
            raise ValidationError("lineal inbreeding: animal is an ancestor of its dam")
        patch["dam_id"] = dam_id
    eff_sire = sire["data"] if sire_id else _parent_data(lookup, entity["data"].get("sire_id"))
    eff_dam = dam["data"] if dam_id else _parent_data(lookup, entity["data"].get("dam_id"))
    if eff_sire and eff_dam and inbreeding_coefficient(eff_sire, eff_dam) > 0.125:
        raise ValidationError("parent update exceeds inbreeding threshold")
    return patch


CUSTOM_CREATE = {'animal': _validate_animal}
CUSTOM_TRANSITIONS = {('pairing', 'approve'): _validate_pairing, ('pairing', 'invalidate_approval'): _validate_invalidate_approval, ('animal', 'update_parents'): _validate_parents_update}


class RuleEngine:
    ALIASES = {'animals': 'animal', 'pairings': 'pairing', 'transfers': 'transfer'}
    INITIAL_STATUS = {'animal': 'active', 'pairing': 'proposed', 'transfer': 'planned'}
    TRANSITIONS = {'animal': {'mark_deceased': (('active',), 'deceased'), 'quarantine_animal': (('active',), 'quarantined'), 'release_quarantine': (('quarantined',), 'active'), 'update_parents': (('active',), 'active')}, 'pairing': {'approve': (('proposed',), 'approved'), 'reject': (('proposed',), 'rejected'), 'complete': (('approved',), 'completed'), 'invalidate_approval': (('approved',), 'proposed')}, 'transfer': {'authorize': (('planned',), 'authorized'), 'ship': (('authorized',), 'in_transit'), 'arrive': (('in_transit',), 'completed')}}
    CREATE_REQUIRED = {'animal': ('name', 'sex'), 'pairing': ('proposed_by',), 'transfer': ('animal_id', 'from_institution', 'to_institution')}
    ACTION_REQUIRED = {('animal', 'mark_deceased'): ('cause',), ('animal', 'quarantine_animal'): ('reason',), ('pairing', 'approve'): ('sire_id', 'dam_id', 'approvals'), ('pairing', 'reject'): ('reason',), ('pairing', 'complete'): ('offspring_ids',), ('transfer', 'authorize'): ('permit_id',), ('transfer', 'ship'): ('transport_id',), ('transfer', 'arrive'): ('arrival_date',)}
    CREATE_ROLES = {'animal': ('admin', 'registrar'), 'pairing': ('admin', 'coordinator'), 'transfer': ('admin', 'registrar')}
    ROLE_ACTIONS = {'mark_deceased': ('admin', 'veterinarian'), 'quarantine_animal': ('admin', 'veterinarian'), 'release_quarantine': ('admin', 'veterinarian'), 'update_parents': ('admin', 'registrar', 'coordinator'), 'invalidate_approval': ('admin', 'coordinator', 'registrar'), 'approve': ('admin', 'coordinator'), 'reject': ('admin', 'coordinator'), 'complete': ('admin', 'coordinator'), 'authorize': ('admin', 'registrar'), 'ship': ('admin', 'registrar'), 'arrive': ('admin', 'registrar')}

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

    @staticmethod
    def _ensure_role(actor, allowed):
        if "*" not in allowed and actor.role not in allowed:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

    @staticmethod
    def _require(data, fields):
        for field in fields:
            value = data.get(field)
            if value is None or value == "" or value == [] or value == {}:
                raise ValidationError("missing required field: " + field)

    def validate_create(self, actor, kind, data, lookup=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        self._ensure_role(actor, self.CREATE_ROLES.get(kind, ("admin",)))
        self._require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = CUSTOM_CREATE.get(kind)
        if custom:
            custom(actor, data, lookup)
        return dict(data)

    def validate_transition(self, actor, entity, action, data, lookup=None):
        kind = self.normalize_kind(entity["kind"])
        transition = self.TRANSITIONS.get(kind, {}).get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition(
                "cannot %s from status %s" % (action, entity["status"])
            )
        allowed_roles = self.ROLE_ACTIONS.get(
            (kind, action), self.ROLE_ACTIONS.get(action, ("admin",))
        )
        self._ensure_role(actor, allowed_roles)
        self._require(data, self.ACTION_REQUIRED.get((kind, action), ()))
        custom = CUSTOM_TRANSITIONS.get((kind, action))
        extra = custom(actor, entity, data, lookup) if custom else {}
        patch = dict(data)
        if extra:
            patch.update(extra)
        return next_status, patch


def _find_one(lookup, kind, field, value):
    if lookup is None:
        return None
    rows = lookup(kind, field, value) or []
    return rows[0] if rows else None


def _date_ordinal(value):
    return datetime.fromisoformat(str(value)[:10]).date().toordinal()
