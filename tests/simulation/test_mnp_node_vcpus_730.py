"""viva-api#730 -- ``_mnp_node_vcpus`` asked Batch a question Batch's API does not have.

``describe_job_definitions(jobDefinitionName=..., revision=...)``: there is no ``revision``
parameter. botocore refused the call client-side, a blanket ``except Exception`` retried the
refusal as if it were eventual consistency, and the method returned ``None`` on every
multi-node composite dispatch from 2026-08-24 to 2026-09-20 -- so ``RAY_SHARDS_DEFAULT`` was never
set and a multi-node run got one head node's worth of actors.

It stayed invisible because every unit test used a fake that accepted any keyword. The fakes
here VALIDATE their keywords against botocore's own service model, offline
(``validate_batch_parameters``). The second test below was committed ahead of the fix as a
strict ``xfail`` (P2.1 PR 6a, where the typed client found the defect).
"""

from typing import Any
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError, ParamValidationError

from tests.simulation.test_ray_backend import _ray_settings, validate_batch_parameters
from viva_api.simulation.simulation_service_ray import SimulationServiceRay


class ValidatingBatch:
    """A fake Batch client that checks each call's keywords the way botocore would."""

    def __init__(self, *, fail_with: list[Exception] | None = None, vcpus: str | None = "16") -> None:
        self.calls: list[dict[str, Any]] = []
        self._fail_with = list(fail_with or [])
        self._vcpus = vcpus

    def describe_job_definitions(self, **kwargs: Any) -> dict[str, Any]:
        validate_batch_parameters("DescribeJobDefinitions", kwargs)  # ParamValidationError, as the real client
        self.calls.append(kwargs)
        if self._fail_with:
            raise self._fail_with.pop(0)
        requirements = [{"type": "MEMORY", "value": "1024"}]
        if self._vcpus is not None:
            requirements.insert(0, {"type": "VCPU", "value": self._vcpus})
        container = {"resourceRequirements": requirements}
        node_properties = {"nodeRangeProperties": [{"targetNodes": "0:", "container": container}]}
        definition = {"jobDefinitionName": "base-mnp-abc", "revision": 3, "nodeProperties": node_properties}
        return {"jobDefinitions": [definition]}


def _vcpus(fake: Any, job_definition: str = "base-mnp-abc:3") -> tuple[int | None, Any]:
    service = SimulationServiceRay()
    with (
        patch("viva_api.simulation.dispatch._seams.get_settings", _ray_settings),
        patch.object(service.batch, "client", return_value=fake),
        patch("viva_api.simulation.dispatch.multi_node.time.sleep") as slept,
    ):
        return service._multi_node()._mnp_node_vcpus(job_definition), slept


def test_the_validating_fake_refuses_what_botocore_refuses() -> None:
    with pytest.raises(ParamValidationError, match='Unknown parameter in input: "revision"'):
        ValidatingBatch().describe_job_definitions(jobDefinitionName="base-mnp-abc", revision=3)
    assert ValidatingBatch().describe_job_definitions(jobDefinitions=["base-mnp-abc:3"])["jobDefinitions"]


def test_the_per_node_vcpus_are_read_from_the_job_definition() -> None:
    fake = ValidatingBatch()
    vcpus, slept = _vcpus(fake)
    assert vcpus == 16
    assert fake.calls == [{"jobDefinitions": ["base-mnp-abc:3"]}]  # name AND revision, in the form the API takes
    slept.assert_not_called()  # it used to sleep 3 s on every dispatch


def test_a_service_error_is_retried_and_then_succeeds() -> None:
    error = {"Error": {"Code": "TooManyRequestsException", "Message": "slow down"}}
    fake = ValidatingBatch(fail_with=[ClientError(error, "DescribeJobDefinitions")])  # type: ignore[arg-type]
    vcpus, slept = _vcpus(fake)
    assert vcpus == 16 and len(fake.calls) == 2
    slept.assert_called_once()


def test_a_wrong_call_is_loud_is_not_retried_and_still_does_not_fail_the_dispatch() -> None:
    """The failure mode of #730 itself, should it come back under another keyword: a programming
    error must not be retried as weather, and must not be a DEBUG line."""

    class WrongCall:
        calls = 0

        def describe_job_definitions(self, **kwargs: Any) -> dict[str, Any]:
            WrongCall.calls += 1
            validate_batch_parameters("DescribeJobDefinitions", {**kwargs, "revision": 3})
            return {}

    # The module's logger is patched rather than read through ``caplog``: other tests in this
    # suite reconfigure logging, and whether a record reaches ``caplog`` then depends on test order.
    with patch("viva_api.simulation.dispatch.multi_node.logger.exception") as logged:
        vcpus, slept = _vcpus(WrongCall())
    assert vcpus is None  # a sizing nicety never fails a submission
    assert WrongCall.calls == 1
    slept.assert_not_called()
    logged.assert_called_once()  # ERROR, with the traceback
    assert "a bug in the call" in logged.call_args.args[0]


def test_a_definition_that_declares_no_vcpus_is_none_without_retrying() -> None:
    fake = ValidatingBatch(vcpus=None)
    vcpus, slept = _vcpus(fake)
    assert vcpus is None and len(fake.calls) == 1
    slept.assert_not_called()
