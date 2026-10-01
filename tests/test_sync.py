import json
import tempfile
import unittest
from pathlib import Path
from threading import Thread
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from src.domain import Actor, ValidationError
from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class SyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.actor = Actor("coordinator", "coordinator")

    def tearDown(self):
        self.tmp.cleanup()

    def _file_a(self):
        return {
            "source": "zoo-a",
            "animals": [
                {"source_id": "SB001", "name": "M-1", "sex": "male"},
                {"source_id": "SB002", "name": "F-1", "sex": "female"},
            ],
            "pairings": [
                {
                    "source_id": "P001",
                    "sire_source_id": "SB001",
                    "dam_source_id": "SB002",
                    "season": "2026-spring",
                    "proposed_by": "zoo-a",
                }
            ],
        }

    def _file_b(self):
        return {
            "source": "zoo-b",
            "animals": [
                {"source_id": "SB001", "name": "M-1", "sex": "male", "color": "dark"},
                {"source_id": "SB002", "name": "F-1", "sex": "female", "color": "light"},
            ],
            "pairings": [
                {
                    "source_id": "Q001",
                    "sire_source_id": "SB001",
                    "dam_source_id": "SB002",
                    "season": "2026-spring",
                    "proposed_by": "zoo-b",
                }
            ],
        }

    def test_duplicate_source_ids_merge_into_one_animal(self):
        first = self.service.sync_import(self.actor, self._file_a())
        second = self.service.sync_import(self.actor, self._file_b())

        self.assertTrue(second["animals"][0]["merged"])
        self.assertEqual(first["animals"][0]["id"], second["animals"][0]["id"])
        self.assertEqual(first["animals"][1]["id"], second["animals"][1]["id"])

        animals = self.service.list("animal")
        self.assertEqual(len(animals), 2)
        # merged record keeps the richer data from both sides
        by_source = {a["data"]["source_id"]: a for a in animals}
        self.assertEqual(by_source["SB001"]["data"]["color"], "dark")
        self.assertEqual(by_source["SB002"]["data"]["color"], "light")
        # the old source id still resolves to the canonical animal
        canonical = self.repo.find_animal_by_source("SB001")
        self.assertEqual(canonical["id"], first["animals"][0]["id"])

    def test_same_pairing_from_both_sides_only_one_effective(self):
        first = self.service.sync_import(self.actor, self._file_a())
        second = self.service.sync_import(self.actor, self._file_b())

        self.assertTrue(second["pairings"][0]["deduplicated"])
        self.assertEqual(first["pairings"][0]["id"], second["pairings"][0]["id"])

        pairings = self.service.list("pairing")
        self.assertEqual(len(pairings), 1)
        pairing = pairings[0]
        self.assertEqual(pairing["data"]["sire_id"], first["animals"][0]["id"])
        self.assertEqual(pairing["data"]["dam_id"], first["animals"][1]["id"])
        self.assertEqual(pairing["data"]["season"], "2026-spring")
        # the other zoo's source id is kept as an alias on the same record
        self.assertEqual(
            self.repo.find_pairing_by_source("Q001")["id"], pairing["id"]
        )

    def test_different_cycles_keep_separate_pairings(self):
        self.service.sync_import(self.actor, self._file_a())
        other = self._file_a()
        other["pairings"][0]["source_id"] = "P002"
        other["pairings"][0]["season"] = "2027-spring"
        self.service.sync_import(self.actor, other)
        self.assertEqual(len(self.service.list("pairing")), 2)

    def test_retry_after_failure_does_not_duplicate(self):
        broken = self._file_a()
        broken["pairings"][0]["dam_source_id"] = "SB999"
        with self.assertRaises(ValidationError):
            self.service.sync_import(self.actor, broken)
        # animals imported before the failure are committed
        self.assertEqual(len(self.service.list("animal")), 2)
        self.assertEqual(len(self.service.list("pairing")), 0)

        fixed = self._file_a()
        summary = self.service.sync_import(self.actor, fixed)
        self.assertEqual(len(self.service.list("animal")), 2)
        self.assertEqual(len(self.service.list("pairing")), 1)
        # animals persisted from the failed attempt merge, pairing is new
        self.assertTrue(summary["animals"][0]["merged"])
        self.assertFalse(summary["pairings"][0]["deduplicated"])

    def test_retrying_same_import_is_idempotent(self):
        first = self.service.sync_import(self.actor, self._file_a())
        second = self.service.sync_import(self.actor, self._file_a())
        self.assertEqual(
            [a["id"] for a in first["animals"]],
            [a["id"] for a in second["animals"]],
        )
        self.assertEqual(first["pairings"][0]["id"], second["pairings"][0]["id"])
        # dedup flags report the merge, but no extra records are created
        self.assertTrue(all(a["merged"] for a in second["animals"]))
        self.assertTrue(all(p["deduplicated"] for p in second["pairings"]))
        self.assertEqual(len(self.service.list("animal")), 2)
        self.assertEqual(len(self.service.list("pairing")), 1)

    def test_batch_idempotency_key_returns_stored_result(self):
        first = self.service.sync_import(self.actor, self._file_a(), batch_id="batch-1")
        second = self.service.sync_import(self.actor, self._file_b(), batch_id="batch-1")
        self.assertEqual(first, second)
        # stored result is retrievable
        stored = self.service.get_sync_batch("batch-1")
        self.assertEqual(stored, first)

    def test_parent_update_invalidates_approved_pairing(self):
        summary = self.service.sync_import(self.actor, self._file_a())
        pairing_id = summary["pairings"][0]["id"]
        sire_id = summary["animals"][0]["id"]

        pairing = self.service.transition(
            self.actor, pairing_id, "approve",
            {"sire_id": sire_id, "dam_id": summary["animals"][1]["id"], "approvals": ["vet-1"]},
        )
        self.assertEqual(pairing["status"], "approved")

        new_sire = self.service.create(
            Actor("admin", "admin"), "animal", {"name": "M-2", "sex": "male"}
        )
        updated = self.service.transition(
            self.actor, sire_id, "update_parents", {"sire_id": new_sire["id"]}
        )
        self.assertEqual(updated["status"], "active")

        pairing = self.service.get(pairing_id)
        self.assertEqual(pairing["status"], "proposed")
        self.assertTrue(pairing["data"]["approval_invalidated"])

        # completion is blocked until re-confirmation
        with self.assertRaises(Exception):
            self.service.transition(
                self.actor, pairing_id, "complete", {"offspring_ids": ["o1"]}
            )
        # re-approval against the updated pedigree works
        pairing = self.service.transition(
            self.actor, pairing_id, "approve",
            {"sire_id": sire_id, "dam_id": summary["animals"][1]["id"], "approvals": ["vet-2"]},
        )
        self.assertEqual(pairing["status"], "approved")
        self.assertFalse(pairing["data"].get("approval_invalidated", False))

    def test_sync_resolves_parents_to_canonical_animals(self):
        payload = {
            "source": "zoo-a",
            "animals": [
                {"source_id": "SB100", "name": "M-100", "sex": "male",
                 "sire_source_id": "SB200", "dam_source_id": "SB201"},
                {"source_id": "SB200", "name": "M-200", "sex": "male"},
                {"source_id": "SB201", "name": "F-201", "sex": "female"},
            ],
            "pairings": [],
        }
        summary = self.service.sync_import(self.actor, payload)
        child = self.repo.find_animal_by_source("SB100")
        sire = self.repo.find_animal_by_source("SB200")
        dam = self.repo.find_animal_by_source("SB201")
        self.assertEqual(child["data"]["sire_id"], sire["id"])
        self.assertEqual(child["data"]["dam_id"], dam["id"])


class SyncHttpTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(repo, RuleEngine())
        rules = RuleEngine()
        self.server = create_server("127.0.0.1", 0, self.service, rules, "static")
        self.port = self.server.server_port
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def _post(self, path, body, batch_id=None):
        headers = {"Content-Type": "application/json", "X-User-Id": "coordinator",
                   "X-Role": "coordinator"}
        if batch_id:
            headers["Idempotency-Key"] = batch_id
        request = Request(
            "http://127.0.0.1:%s%s" % (self.port, path),
            data=json.dumps(body).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            return error.code, json.loads(error.read().decode("utf-8"))

    def _get(self, path):
        request = Request("http://127.0.0.1:%s%s" % (self.port, path))
        with urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))

    def test_sync_endpoint_is_retryable(self):
        body = {
            "source": "zoo-a",
            "animals": [
                {"source_id": "SB001", "name": "M-1", "sex": "male"},
                {"source_id": "SB002", "name": "F-1", "sex": "female"},
            ],
            "pairings": [
                {"source_id": "P001", "sire_source_id": "SB001",
                 "dam_source_id": "SB002", "season": "2026-spring"},
            ],
        }
        status, first = self._post("/api/sync", body, batch_id="http-batch")
        self.assertEqual(status, 200)
        status, second = self._post("/api/sync", body, batch_id="http-batch")
        self.assertEqual(status, 200)
        self.assertEqual(first, second)

        stored = self._get("/api/sync/http-batch")
        self.assertEqual(stored, first)

        listing = self._get("/api/pairings")
        self.assertEqual(len(listing["items"]), 1)


if __name__ == "__main__":
    unittest.main()
