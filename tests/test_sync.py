import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.domain import Actor, ConflictError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class SyncTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.coordinator = Actor("breed-coord", "coordinator")
        self.registrar = Actor("zoo-a-registrar", "registrar")
        self.admin = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def _sync(self, key, source, animals=None, pairings=None, actor=None):
        return self.service.import_sync_batch(
            actor or self.coordinator, key, source,
            animals=animals, pairings=pairings,
        )

    def _item_map(self, result):
        return {item["ref"]: item for item in result["items"]}


class BatchImportTest(SyncTestBase):
    def test_imports_animals_and_pairings_in_one_batch(self):
        result = self._sync("batch-1", "Zoo-B", animals=[
            {"source_id": "b-m1", "name": "Ben", "sex": "male"},
            {"source_id": "b-f1", "name": "Bella", "sex": "female"},
        ], pairings=[
            {"source_id": "b-p1", "sire_ref": "b-m1", "dam_ref": "b-f1",
             "status": "approved", "approvals": ["vet-2"]},
        ])
        self.assertEqual(result["status"], "completed")
        items = self._item_map(result)
        sire = items["b-m1"]["canonical_id"]
        dam = items["b-f1"]["canonical_id"]
        pairing_id = items["b-p1"]["canonical_id"]
        pairing = self.service.get(pairing_id)
        self.assertEqual(pairing["status"], "approved")
        self.assertEqual(pairing["data"]["sire_id"], sire)
        self.assertEqual(pairing["data"]["dam_id"], dam)
        slot = self.service.pairing_slot(sire, dam)
        self.assertEqual(slot["pairing_id"], pairing_id)

    def test_rejected_sync_role(self):
        with self.assertRaises(PermissionDenied):
            self._sync("batch-x", "Zoo-B",
                       animals=[{"source_id": "x", "name": "X", "sex": "male"}],
                       actor=Actor("viewer", "viewer"))


class MergeTest(SyncTestBase):
    def test_same_name_duplicates_merge_into_one_animal(self):
        first = self._sync("m1", "Zoo-A", animals=[
            {"source_id": "a-1", "id": "local-lion", "name": "Leo", "sex": "male"},
        ])
        result = self._sync("m2", "Zoo-B", animals=[
            {"source_id": "b-1", "name": "Leo", "sex": "male"},
        ])
        canonical = self._item_map(result)["b-1"]["canonical_id"]
        self.assertEqual(canonical, first["items"][0]["canonical_id"])
        animals = [a for a in self.service.list("animal")
                   if a["status"] != "merged"]
        self.assertEqual(len(animals), 1)

        # When the foreign record also carries a local id, that id is kept as
        # a redirect to the canonical animal.
        second = self._sync("m3", "Zoo-C", animals=[
            {"source_id": "c-1", "id": "foreign-lion", "name": "Leo",
             "sex": "male"},
        ])
        self.assertEqual(
            self._item_map(second)["c-1"]["canonical_id"], canonical
        )
        via_redirect = self.service.get("foreign-lion")
        self.assertEqual(via_redirect["id"], canonical)
        self.assertEqual(via_redirect["status"], "active")
        raw = self.repo.get_entity("foreign-lion")
        self.assertEqual(raw["status"], "merged")

    def test_old_number_destinations_are_kept(self):
        self._sync("m1", "Zoo-A", animals=[
            {"source_id": "a-1", "id": "lion-1", "name": "Leo", "sex": "male"},
        ])
        self._sync("m2", "Zoo-B", animals=[
            {"source_id": "b-1", "id": "lion-2", "name": "Leo", "sex": "male"},
        ])
        refs = {
            (r["source_system"], r["source_id"]): r
            for r in self.service.source_refs()
        }
        self.assertEqual(refs[("Zoo-A", "a-1")]["canonical_id"], "lion-1")
        self.assertEqual(refs[("Zoo-B", "b-1")]["canonical_id"], "lion-1")
        self.assertEqual(refs[("Zoo-B", "lion-2")]["merged_into"], "lion-1")
        self.assertEqual(self.repo.get_redirect("lion-2"), "lion-1")

    def test_pairing_follows_merged_animal(self):
        # Zoo-B already breeds an animal (sire-b) paired with its dam.  Zoo-A
        # later delivers the same individual under sire-a; the pairing and its
        # claimed slot must follow the surviving canonical animal.
        self._sync("m1", "Zoo-B", animals=[
            {"source_id": "b-sire", "id": "sire-b", "name": "Sam",
             "sex": "male"},
            {"source_id": "b-dam", "id": "dam-b", "name": "Dora",
             "sex": "female"},
        ], pairings=[
            {"source_id": "p-b", "sire_ref": "b-sire", "dam_ref": "b-dam",
             "status": "approved"},
        ])
        self._sync("m2", "Zoo-A", animals=[
            {"source_id": "a-sire", "id": "sire-a", "name": "Sam",
             "sex": "male"},
        ])
        pairing = self.service.get("pairing-Zoo-B-p-b")
        self.assertEqual(pairing["data"]["sire_id"], "sire-b")
        slot = self.service.pairing_slot("sire-b", "dam-b")
        self.assertEqual(slot["pairing_id"], "pairing-Zoo-B-p-b")
        self.assertIsNone(self.repo.get_slot("sire-a", "dam-b"))
        animals = [a for a in self.service.list("animal")
                   if a["status"] != "merged"]
        self.assertEqual(len(animals), 2)
        # Breeding arrangements resolve to the merged individual through
        # either former number.
        via_other = self.service.get("sire-a")
        via_canonical = self.service.get("sire-b")
        self.assertEqual(via_other["id"], via_canonical["id"])

    def test_studbook_id_and_alias_links_merge(self):
        self.service.create(self.admin, "animal",
                            {"name": "Chip", "sex": "female",
                             "studbook_id": "STUD-99"})
        result = self._sync("m3", "Zoo-B", animals=[
            {"source_id": "b-9", "name": "Different Label", "sex": "female",
             "studbook_id": "STUD-99"},
        ])
        canonical = self._item_map(result)["b-9"]["canonical_id"]
        self.assertEqual(self.service.get(canonical)["data"]["studbook_id"],
                         "STUD-99")
        self.assertEqual(len([a for a in self.service.list("animal")
                              if a["status"] != "merged"]), 1)

    def test_duplicate_source_ids_inside_one_batch_rejected(self):
        from src.domain import ValidationError
        with self.assertRaises(ValidationError):
            self._sync("dup", "Zoo-B", animals=[
                {"source_id": "same-1", "name": "Twin", "sex": "female",
                 "birth_date": "2024-01-01"},
                {"source_id": "same-1", "name": "Twin", "sex": "female",
                 "studbook_id": "S-7"},
            ])


class ConcurrentPairingTest(SyncTestBase):
    def test_two_zoos_submit_same_pairing_only_one_wins(self):
        self.service.create(self.admin, "animal",
                            {"id": "shared-sire", "name": "S", "sex": "male"})
        self.service.create(self.admin, "animal",
                            {"id": "shared-dam", "name": "D", "sex": "female"})

        barrier = threading.Barrier(2)

        def submit(source, key):
            barrier.wait()
            return self._sync(key, source, pairings=[
                {"source_id": source + "-p", "sire_ref": "shared-sire",
                 "dam_ref": "shared-dam", "status": "approved"},
            ])

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda args: submit(*args),
                                    [("Zoo-A", "k-a"), ("Zoo-B", "k-b")]))
        winners = set()
        for result in results:
            self.assertEqual(result["status"], "completed")
            winners.add(result["items"][0]["canonical_id"])
        self.assertEqual(len(winners), 1)
        pairings = [p for p in self.service.list("pairing")
                    if p["status"] != "rejected"]
        self.assertEqual(len(pairings), 1)

    def test_second_approval_of_occupied_slot_conflicts(self):
        sire = self.service.create(self.admin, "animal",
                                   {"name": "S", "sex": "male"})
        dam = self.service.create(self.admin, "animal",
                                  {"name": "D", "sex": "female"})
        pairing = self.service.create(self.coordinator, "pairing",
                                      {"proposed_by": "coord"})
        self.service.transition(
            self.coordinator, pairing["id"], "approve",
            {"sire_id": sire["id"], "dam_id": dam["id"],
             "approvals": ["vet-1"]},
        )
        rival = self.service.create(self.coordinator, "pairing",
                                    {"proposed_by": "coord"})
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, rival["id"], "approve",
                {"sire_id": sire["id"], "dam_id": dam["id"],
                 "approvals": ["vet-2"]},
            )


class LineageInvalidationTest(SyncTestBase):
    def _approved_pairing(self):
        result = self._sync("line-1", "Zoo-A", animals=[
            {"source_id": "sire", "name": "Sire", "sex": "male"},
            {"source_id": "dam", "name": "Dam", "sex": "female"},
        ], pairings=[
            {"source_id": "p1", "sire_ref": "sire", "dam_ref": "dam",
             "status": "approved", "approvals": ["vet-1"]},
        ])
        items = self._item_map(result)
        return items["p1"]["canonical_id"]

    def test_approved_pairing_needs_reconfirmation_after_lineage_update(self):
        pairing_id = self._approved_pairing()
        self._sync("line-2", "Zoo-B", animals=[
            {"source_id": "grand-sire", "name": "Grandpa", "sex": "male"},
            {"source_id": "sire", "name": "Sire", "sex": "male",
             "sire_ref": "grand-sire"},
        ])
        pairing = self.service.get(pairing_id)
        self.assertEqual(pairing["status"], "needs_confirmation")
        self.assertEqual(pairing["data"]["approvals"], [])
        self.assertIn("lineage", pairing["data"]
                      ["needs_reconfirmation_reason"])
        history = pairing["data"]["approval_history"]
        self.assertEqual(history[-1]["approvals"], ["vet-1"])
        self.service.transition(
            self.coordinator, pairing_id, "approve",
            {"sire_id": pairing["data"]["sire_id"],
             "dam_id": pairing["data"]["dam_id"],
             "approvals": ["vet-3"]},
        )
        self.assertEqual(self.service.get(pairing_id)["status"], "approved")

    def test_unchanged_parent_replay_keeps_approval(self):
        pairing_id = self._approved_pairing()
        grand = self._sync("line-2", "Zoo-B", animals=[
            {"source_id": "grand-sire", "name": "Grandpa", "sex": "male"},
            {"source_id": "sire", "name": "Sire", "sex": "male",
             "sire_ref": "grand-sire"},
        ])
        self.assertEqual(self.service.get(pairing_id)["status"],
                         "needs_confirmation")
        # Reconfirm.
        pairing = self.service.get(pairing_id)
        self.service.transition(
            self.coordinator, pairing_id, "approve",
            {"sire_id": pairing["data"]["sire_id"],
             "dam_id": pairing["data"]["dam_id"],
             "approvals": ["vet-3"]},
        )
        # Replay the exact same batch: lineage did not change again.
        self._sync("line-2-retry", "Zoo-B", animals=[
            {"source_id": "grand-sire", "name": "Grandpa", "sex": "male"},
            {"source_id": "sire", "name": "Sire", "sex": "male",
             "sire_ref": "grand-sire"},
        ])
        self.assertEqual(self.service.get(pairing_id)["status"], "approved")

    def test_synced_pairing_with_changed_parents_loses_approval(self):
        result = self._sync("lp-1", "Zoo-A", animals=[
            {"source_id": "s1", "name": "S1", "sex": "male"},
            {"source_id": "d1", "name": "D1", "sex": "female"},
            {"source_id": "d2", "name": "D2", "sex": "female"},
        ], pairings=[
            {"source_id": "pp", "sire_ref": "s1", "dam_ref": "d1",
             "status": "approved"},
        ])
        pairing_id = self._item_map(result)["pp"]["canonical_id"]
        self.assertEqual(self.service.get(pairing_id)["status"], "approved")
        self._sync("lp-2", "Zoo-B", pairings=[
            {"source_id": "pp", "sire_ref": "s1", "dam_ref": "d2",
             "status": "approved"},
        ])
        pairing = self.service.get(pairing_id)
        self.assertEqual(pairing["status"], "needs_confirmation")
        self.assertEqual(pairing["data"]["dam_id"],
                         self._item_map(self.service.sync_status("lp-1"))
                         ["d2"]["canonical_id"])


class RetryTest(SyncTestBase):
    def test_retry_after_failure_resumes_without_duplicates(self):
        calls = {"count": 0}

        def boom(kind, ref, record):
            if ref == "p-fail" and calls["count"] == 0:
                calls["count"] += 1
                raise RuntimeError("simulated transport failure")

        self.service.sync.before_item = boom
        animals = [
            {"source_id": "m-sire", "name": "Retry Sire", "sex": "male"},
            {"source_id": "m-dam", "name": "Retry Dam", "sex": "female"},
        ]
        pairings = [
            {"source_id": "p-fail", "sire_ref": "m-sire",
             "dam_ref": "m-dam", "status": "approved"},
        ]
        with self.assertRaises(RuntimeError):
            self._sync("retry-1", "Zoo-B", animals=animals, pairings=pairings)

        # Animals were already checkpointed; the pairing transaction rolled
        # back completely.
        self.service.sync.before_item = None
        result = self._sync("retry-1", "Zoo-B", animals=animals,
                            pairings=pairings)
        self.assertEqual(result["status"], "completed")
        active_animals = [a for a in self.service.list("animal")
                          if a["status"] != "merged"]
        self.assertEqual(len(active_animals), 2)
        pairings_all = self.service.list("pairing")
        self.assertEqual(len(pairings_all), 1)
        items = self._item_map(result)
        self.assertEqual(items["p-fail"]["status"], "completed")
        sire = items["m-sire"]["canonical_id"]
        dam = items["m-dam"]["canonical_id"]
        slot = self.service.pairing_slot(sire, dam)
        self.assertEqual(slot["pairing_id"],
                         items["p-fail"]["canonical_id"])

        # A second replay of the finished batch returns the same result and
        # creates nothing new.
        again = self._sync("retry-1", "Zoo-B", animals=animals,
                           pairings=pairings)
        self.assertTrue(again["already_complete"])
        self.assertEqual(len([a for a in self.service.list("animal")
                              if a["status"] != "merged"]), 2)
        self.assertEqual(len(self.service.list("pairing")), 1)

    def test_failed_item_recorded_but_other_items_continue(self):
        result = self._sync("partial", "Zoo-B", animals=[
            {"source_id": "ok", "name": "Fine", "sex": "female"},
        ], pairings=[
            {"source_id": "broken", "sire_ref": "missing-sire",
             "dam_ref": "ok"},
        ])
        self.assertEqual(result["status"], "failed")
        items = self._item_map(result)
        self.assertEqual(items["ok"]["status"], "completed")
        self.assertEqual(items["broken"]["status"], "failed")
        self.assertIn("resolve", items["broken"]["detail"]["error"])

    def test_same_sync_key_concurrent_replays_stay_idempotent(self):
        animals = [{"source_id": "a", "name": "Once", "sex": "male"}]
        pairings = []
        barrier = threading.Barrier(2)

        def submit():
            barrier.wait()
            return self._sync("concurrent-key", "Zoo-B",
                              animals=animals, pairings=pairings)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: submit(), range(2)))
        ids = {r["items"][0]["canonical_id"] for r in results}
        self.assertEqual(len(ids), 1)
        self.assertEqual(len([a for a in self.service.list("animal")
                              if a["status"] != "merged"]), 1)


if __name__ == "__main__":
    unittest.main()
