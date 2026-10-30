"""Exercise actual treatment dispatch against a non-network native stand-in."""
import asyncio
import importlib.util
import os
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/eclipse_hivemind/nodes"))


def module(name, **attributes):
    result = ModuleType(name)
    result.__dict__.update(attributes)
    return result


class NativeAverager:
    def __init__(self, *args, **kwargs):
        self.arguments = args, kwargs

    async def _aggregate_with_group(self, group, *args, **kwargs):
        self.native_arguments = group, args, kwargs
        return "native-result"


class NativeRuntime:
    def __init__(self, settings, malicious, trace_factory):
        self.settings, self.malicious = settings, malicious
        self.trace = Mock()
        self.trace.register_process.side_effect = lambda process: process
        self.identity = {"experiment_id": "run"}

    def after_barrier(self, dht):
        pass

    def close(self):
        pass

    def report_finished(self):
        pass


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        from shared.eclipse import Settings, flag
        self.settings_factory = Mock()
        self.honest = Mock()
        self.client = Mock()
        replacements = {
            "honest_runner": module("honest_runner", main=self.honest),
            "hivemind": module("hivemind"), "hivemind.dht": module("hivemind.dht"),
            "hivemind.optim": module("hivemind.optim"),
            "hivemind.optim.grad_averager": module("hivemind.optim.grad_averager", GradientAverager=NativeAverager),
            "hivemind.dht.routing": module("hivemind.dht.routing", DHTID=int),
            "shared.dht": module("shared.dht", start_dht=Mock()),
            "shared.eclipse_dht": module("shared.eclipse_dht", ObservedDHT=object, metadata=Mock()),
            "shared.eclipse_averaging": module("shared.eclipse_averaging", ObservedGradientAverager=NativeAverager),
            "shared.eclipse_runtime": module("shared.eclipse_runtime", Runtime=NativeRuntime),
            "coordinated_attacker.coordinator": module("coordinated_attacker.coordinator", CoordinatorClient=self.client),
            "coordinated_attacker.policy": module("coordinated_attacker.policy", AttackPlan=lambda *a: a,
                      CoordinatedGradientAverager=type("NativeCoordinated", (NativeAverager,), {}),
                      CoordinationPolicy=lambda *a: a),
        }
        self.patch = patch.dict(sys.modules, replacements)
        self.patch.start()
        spec = importlib.util.spec_from_file_location("factorial.runtime_probe", ROOT / "src/eclipse_hivemind/nodes/factorial/runtime.py")
        self.runtime = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.runtime)

    def tearDown(self):
        self.patch.stop()

    def test_native_and_coordinated_factories_are_selected_only_for_owners(self):
        roles = {f"peer-{i}": dict(peer_id=f"peer-{i}", coordination_role="recruiter" if i < 3 else None,
                                    focal_victim=i == 3) for i in range(20)}
        for role in ("normal", "victim", "support", "recruiter"):
            for coordination in (False, True):
                with patch.dict(os.environ, {"PARAM_FACTOR_FILTERING": "false", "PARAM_FACTOR_POISONING": "false",
                                             "PARAM_COORDINATOR_ENDPOINT": "http://unused"}):
                    runtime = self.runtime.FactorialRuntime(SimpleNamespace(averager="standard", group_key="key"), role, coordination)
                    runtime.roles = roles
                    runtime.after_barrier(None)
                    averager = runtime.averager_factory()
                expected = (self.runtime.AuditedCoordinatedAverager if coordination and role in {"support", "recruiter"}
                            else self.runtime.ContributionOnlyAverager if role == "normal" else self.runtime.AuditedAverager)
                self.assertIs(type(averager), expected)

    def test_actual_inversion_changes_gradients_once_without_touching_missing_gradients(self):
        class Gradient:
            value = 3
            def neg_(self):
                self.value = -self.value
        gradient = Gradient()
        model = SimpleNamespace(parameters=lambda: [SimpleNamespace(grad=gradient), SimpleNamespace(grad=None)])
        runtime = object.__new__(self.runtime.FactorialRuntime)
        runtime.transform_calls = 0
        runtime.reverse_gradients(model)
        self.assertEqual(gradient.value, -3)
        self.assertEqual(runtime.transform_calls, 1)

    def test_weight_observation_preserves_native_weight_and_call_result(self):
        averager = self.runtime.AuditedAverager()
        averager.eclipse_trace, averager.peer_id = Mock(), "peer"
        group = SimpleNamespace(group_id=b"group")
        result = asyncio.run(averager._aggregate_with_group(group, 17, weight=96.0))
        self.assertEqual(result, "native-result")
        self.assertEqual(averager.native_arguments, (group, (17,), {"weight": 96.0}))
        self.assertEqual(averager.eclipse_trace.emit.call_args.kwargs["weight"], 96.)

    def test_dht_receives_a_separate_shutdown_budget(self):
        runtime = self.runtime.FactorialRuntime(SimpleNamespace(replicas=5, placement="random"), "normal", False)
        dht = Mock()
        dht.run_coroutine.return_value = {}
        with patch.object(self.runtime, "start_dht", return_value=dht) as start:
            self.assertIs(runtime.start_dht(), dht)
        self.assertEqual(start.call_args.kwargs["shutdown_timeout"], 30)
        self.assertEqual(start.call_args.kwargs["num_replicas"], 5)
        self.assertIs(start.call_args.kwargs["dht_class"], self.runtime.AuditedDHT)


if __name__ == "__main__":
    unittest.main()
