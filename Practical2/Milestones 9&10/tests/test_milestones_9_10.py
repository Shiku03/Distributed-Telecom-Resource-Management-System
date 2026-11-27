import unittest
from common.lifecycle import InvalidTransition, transition
from common.service_registry import ServiceRegistry, NameConflict


class LifecycleTests(unittest.TestCase):
    def test_normal_process_lifecycle(self):
        state = "CREATED"
        for target in ("ALLOCATING", "RUNNING", "RESIZING", "RUNNING", "DRAINING", "TERMINATED"):
            state = transition(state, target)
        self.assertEqual(state, "TERMINATED")

    def test_invalid_transition_rejected(self):
        with self.assertRaises(InvalidTransition):
            transition("TERMINATED", "RUNNING")


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.registry = ServiceRegistry()

    def test_register_resolve_relocate_and_unregister(self):
        self.registry.register("service://telerm/upf/1", "S00001", "UPF", "edge-A", "127.0.0.1", 6101)
        self.assertEqual(self.registry.resolve("service://telerm/upf/1")["site_id"], "edge-A")
        self.registry.update_location("S00001", "core-1", "127.0.0.1", 6103)
        self.assertEqual(self.registry.resolve("service://telerm/upf/1")["site_id"], "core-1")
        self.registry.unregister_instance("S00001")
        self.assertIsNone(self.registry.resolve("service://telerm/upf/1"))

    def test_duplicate_name_rejected(self):
        self.registry.register("service://telerm/upf/1", "S00001", "UPF", "edge-A", "127.0.0.1", 6101)
        with self.assertRaises(NameConflict):
            self.registry.register("service://telerm/upf/1", "S00002", "UPF", "edge-B", "127.0.0.1", 6102)

    def test_unavailable_name_not_resolved(self):
        self.registry.register("service://telerm/upf/1", "S00001", "UPF", "edge-A", "127.0.0.1", 6101)
        self.registry.set_state("S00001", "UNAVAILABLE")
        self.assertIsNone(self.registry.resolve("service://telerm/upf/1"))


if __name__ == "__main__":
    unittest.main()
