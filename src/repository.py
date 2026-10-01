import json
import sqlite3
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
                CREATE TABLE IF NOT EXISTS animal_sources (
                    source_key TEXT PRIMARY KEY,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pairing_sources (
                    source_key TEXT PRIMARY KEY,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pairing_keys (
                    pairing_key TEXT PRIMARY KEY,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sync_batches (
                    batch_key TEXT PRIMARY KEY,
                    actor_id TEXT NOT NULL,
                    result TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
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

    def find_animal_by_source(self, source_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM animal_sources WHERE source_key = ?",
                (str(source_key),),
            ).fetchone()
        return self.get_entity(row["entity_id"]) if row else None

    def create_animal_if_absent(self, source_key, entity_id, data, actor_id):
        """Atomically bind a source id to an animal, creating it on first sight.

        Returns (entity, created). A duplicate source key resolves to the
        already-bound entity so a retry never creates a second animal.
        """
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT entity_id FROM animal_sources WHERE source_key = ?",
                (str(source_key),),
            ).fetchone()
            if row:
                connection.commit()
                return self.get_entity(row["entity_id"]), False
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, 'animal', 'active', 1, ?, ?, ?, ?)",
                (entity_id, payload, actor_id, now, now),
            )
            connection.execute(
                "INSERT INTO animal_sources(source_key, entity_id, created_at) VALUES (?, ?, ?)",
                (str(source_key), entity_id, now),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id), True

    def find_pairing_by_source(self, source_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM pairing_sources WHERE source_key = ?",
                (str(source_key),),
            ).fetchone()
        return self.get_entity(row["entity_id"]) if row else None

    def find_pairing_by_key(self, pairing_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM pairing_keys WHERE pairing_key = ?",
                (str(pairing_key),),
            ).fetchone()
        return self.get_entity(row["entity_id"]) if row else None

    def create_pairing_if_absent(self, source_key, pairing_key, entity_id, data, actor_id):
        """Atomically create a pairing on first sight of its source id or pair key.

        Returns (entity, created). A matching pair key (same sire, dam and
        cycle) or source id resolves to the existing pairing so both zoos
        submitting the same pairing only ever produce one effective record.
        """
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT entity_id FROM pairing_keys WHERE pairing_key = ?",
                (str(pairing_key),),
            ).fetchone()
            if not row:
                row = connection.execute(
                    "SELECT entity_id FROM pairing_sources WHERE source_key = ?",
                    (str(source_key),),
                ).fetchone()
            if row:
                connection.commit()
                return self.get_entity(row["entity_id"]), False
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, 'pairing', 'proposed', 1, ?, ?, ?, ?)",
                (entity_id, payload, actor_id, now, now),
            )
            connection.execute(
                "INSERT INTO pairing_sources(source_key, entity_id, created_at) VALUES (?, ?, ?)",
                (str(source_key), entity_id, now),
            )
            connection.execute(
                "INSERT INTO pairing_keys(pairing_key, entity_id, created_at) VALUES (?, ?, ?)",
                (str(pairing_key), entity_id, now),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id), True

    def bind_pairing_source(self, source_key, entity_id):
        """Record an alias source id for a pairing; returns the bound entity id."""
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO pairing_sources(source_key, entity_id, created_at) "
                "VALUES (?, ?, ?)",
                (str(source_key), entity_id, utcnow()),
            )
            row = connection.execute(
                "SELECT entity_id FROM pairing_sources WHERE source_key = ?",
                (str(source_key),),
            ).fetchone()
        return row["entity_id"] if row else entity_id

    def get_sync_batch(self, batch_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT result FROM sync_batches WHERE batch_key = ?",
                (str(batch_key),),
            ).fetchone()
        return json.loads(row["result"]) if row else None

    def save_sync_batch(self, batch_key, actor_id, result):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO sync_batches(batch_key, actor_id, result, created_at) "
                "VALUES (?, ?, ?, ?)",
                (str(batch_key), actor_id, json.dumps(result, ensure_ascii=False, sort_keys=True), utcnow()),
            )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
