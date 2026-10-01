import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS source_refs (
                    source_system TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    canonical_id TEXT NOT NULL,
                    merged_into TEXT,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(source_system, source_id)
                );
                CREATE INDEX IF NOT EXISTS idx_source_refs_canonical
                    ON source_refs(canonical_id);
                CREATE TABLE IF NOT EXISTS entity_redirects (
                    old_id TEXT PRIMARY KEY,
                    canonical_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pairing_slots (
                    sire_id TEXT NOT NULL,
                    dam_id TEXT NOT NULL,
                    pairing_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(sire_id, dam_id)
                );
                CREATE TABLE IF NOT EXISTS sync_batches (
                    sync_key TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    source_system TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_items (
                    sync_key TEXT NOT NULL,
                    item_ref TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    canonical_id TEXT,
                    detail TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(sync_key, item_ref)
                );
                CREATE INDEX IF NOT EXISTS idx_sync_items_status
                    ON sync_items(sync_key, status);
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    @contextmanager
    def transaction(self):
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _row_to_entity(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def conn_get_entity(self, connection, entity_id):
        row = connection.execute(
            "SELECT * FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        return self._row_to_entity(row) if row else None

    def conn_upsert_entity(self, connection, entity_id, kind, status, data,
                           actor_id, bump_version=False):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        row = connection.execute(
            "SELECT id FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
            return 1
        if bump_version:
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ?",
                (status, payload, now, entity_id),
            )
        else:
            connection.execute(
                "UPDATE entities SET status = ?, data = ?, updated_at = ? WHERE id = ?",
                (status, payload, now, entity_id),
            )
        row = connection.execute(
            "SELECT version FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        return int(row["version"])

    def conn_append_audit(self, connection, entity_id, actor_id, actor_role,
                          action, from_status, to_status, detail):
        connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_id,
                actor_id,
                actor_role,
                action,
                from_status,
                to_status,
                json.dumps(detail, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )

    def conn_save_source_ref(self, connection, source_system, source_id, kind,
                             canonical_id, merged_into=None):
        connection.execute(
            "INSERT INTO source_refs(source_system, source_id, kind, canonical_id, merged_into, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(source_system, source_id) DO UPDATE SET "
            "kind = excluded.kind, canonical_id = excluded.canonical_id, "
            "merged_into = COALESCE(excluded.merged_into, source_refs.merged_into)",
            (source_system, source_id, kind, canonical_id, merged_into, utcnow()),
        )

    def get_source_ref(self, source_system, source_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM source_refs WHERE source_system = ? AND source_id = ?",
                (source_system, source_id),
            ).fetchone()
        return dict(row) if row else None

    def find_source_ref_any_system(self, source_id, kind=None):
        with self._connect() as connection:
            if kind:
                rows = connection.execute(
                    "SELECT * FROM source_refs WHERE source_id = ? AND kind = ? "
                    "ORDER BY source_system",
                    (source_id, kind),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM source_refs WHERE source_id = ? "
                    "ORDER BY source_system",
                    (source_id,),
                ).fetchall()
        return [dict(row) for row in rows]

    def list_source_refs(self, canonical_id=None):
        with self._connect() as connection:
            if canonical_id:
                rows = connection.execute(
                    "SELECT * FROM source_refs WHERE canonical_id = ? "
                    "ORDER BY source_system, source_id",
                    (canonical_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM source_refs ORDER BY canonical_id, source_system, source_id"
                ).fetchall()
        return [dict(row) for row in rows]

    def conn_save_redirect(self, connection, old_id, canonical_id, reason):
        connection.execute(
            "INSERT INTO entity_redirects(old_id, canonical_id, reason, created_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(old_id) DO UPDATE SET canonical_id = excluded.canonical_id, "
            "reason = excluded.reason",
            (old_id, canonical_id, reason, utcnow()),
        )

    def get_redirect(self, old_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT canonical_id FROM entity_redirects WHERE old_id = ?",
                (old_id,),
            ).fetchone()
        return row["canonical_id"] if row else None

    def conn_occupy_slot(self, connection, sire_id, dam_id, pairing_id, status):
        now = utcnow()
        connection.execute(
            "INSERT INTO pairing_slots(sire_id, dam_id, pairing_id, status, created_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(sire_id, dam_id) DO UPDATE SET "
            "status = excluded.status, pairing_id = pairing_slots.pairing_id",
            (sire_id, dam_id, pairing_id, status, now),
        )

    def conn_release_slot(self, connection, sire_id, dam_id):
        connection.execute(
            "DELETE FROM pairing_slots WHERE sire_id = ? AND dam_id = ?",
            (sire_id, dam_id),
        )

    def conn_get_slot(self, connection, sire_id, dam_id):
        row = connection.execute(
            "SELECT * FROM pairing_slots WHERE sire_id = ? AND dam_id = ?",
            (sire_id, dam_id),
        ).fetchone()
        return dict(row) if row else None

    def get_slot(self, sire_id, dam_id):
        with self._connect() as connection:
            return self.conn_get_slot(connection, sire_id, dam_id)

    def conn_active_pairings(self, connection):
        rows = connection.execute(
            "SELECT * FROM entities WHERE kind = 'pairing' "
            "AND status IN ('proposed', 'approved', 'needs_confirmation')"
        ).fetchall()
        return [self._row_to_entity(row) for row in rows]

    def register_batch(self, sync_key, source_system):
        now = utcnow()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO sync_batches(sync_key, status, source_system, created_at, updated_at) "
                "VALUES (?, 'in_progress', ?, ?, ?) "
                "ON CONFLICT(sync_key) DO NOTHING",
                (sync_key, source_system, now, now),
            )
            row = connection.execute(
                "SELECT * FROM sync_batches WHERE sync_key = ?", (sync_key,)
            ).fetchone()
            prior = dict(row)
        return prior

    def set_batch_status(self, sync_key, status):
        with self._connect() as connection:
            connection.execute(
                "UPDATE sync_batches SET status = ?, updated_at = ? WHERE sync_key = ?",
                (status, utcnow(), sync_key),
            )

    def get_batch(self, sync_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM sync_batches WHERE sync_key = ?", (sync_key,)
            ).fetchone()
        return dict(row) if row else None

    def get_item(self, sync_key, item_ref):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM sync_items WHERE sync_key = ? AND item_ref = ?",
                (sync_key, item_ref),
            ).fetchone()
        item = dict(row) if row else None
        if item:
            item["detail"] = json.loads(item["detail"])
        return item

    def list_items(self, sync_key):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM sync_items WHERE sync_key = ? ORDER BY item_ref",
                (sync_key,),
            ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["detail"] = json.loads(item["detail"])
        return items

    def conn_upsert_item(self, connection, sync_key, item_ref, kind, status,
                         canonical_id, detail):
        connection.execute(
            "INSERT INTO sync_items(sync_key, item_ref, kind, status, canonical_id, detail, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(sync_key, item_ref) DO UPDATE SET status = excluded.status, "
            "canonical_id = COALESCE(excluded.canonical_id, sync_items.canonical_id), "
            "detail = excluded.detail, updated_at = excluded.updated_at",
            (sync_key, item_ref, kind, status, canonical_id,
             json.dumps(detail, ensure_ascii=False, sort_keys=True), utcnow()),
        )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
