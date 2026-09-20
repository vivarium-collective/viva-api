"""viva-api#730 -- ``_mnp_node_vcpus`` asks Batch a question Batch's API does not have.

``describe_job_definitions(jobDefinitionName=..., revision=...)``: there is no ``revision``
parameter. botocore refuses the call client-side, the method's ``except Exception`` swallows
the refusal, and it has returned ``None`` on every multi-node composite dispatch since it
landed. Every unit test used a ``MagicMock`` client, which accepts any keyword -- so here the
fake VALIDATES its keywords against botocore's own service model, offline.

The defect was found by the typed client (``docs/plan-core.md`` P2.1, PR 6a) and deliberately not
fixed there: fixing it changes a dispatch's shard count and wants a before/after. This test is
the fix's acceptance test, committed ahead of it as a strict ``xfail``: when the call is
corrected it passes, strict-xfail turns that into a failure, and whoever fixed it deletes the
marker here and the ``# type: ignore[call-arg]`` there in the same PR.
"""

from typing import Any
from unittest.mock import patch

import botocore.session
import pytest
from botocore.validate import validate_parameters

from tests.simulation.test_ray_backend import _ray_settings
from viva_api.simulation.simulation_service_ray import SimulationServiceRay

_BATCH_MODEL = botocore.session.get_session().get_service_model("batch")


class ValidatingBatch:
    """A fake Batch client that checks each call's keywords the way botocore would."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def describe_job_definitions(self, **kwargs: Any) -> dict[str, Any]:
        shape = _BATCH_MODEL.operation_model("DescribeJobDefinitions").input_shape
        assert shape is not None
        validate_parameters(kwargs, shape)  # raises ParamValidationError, as the real client does
        self.calls.append(kwargs)
        container = {"resourceRequirements": [{"type": "VCPU", "value": "16"}, {"type": "MEMORY", "value": "1024"}]}
        node_properties = {"nodeRangeProperties": [{"targetNodes": "0:", "container": container}]}
        return {
            "jobDefinitions": [{"jobDefinitionName": "base-mnp-abc", "revision": 3, "nodeProperties": node_properties}]
        }


def test_the_validating_fake_refuses_what_botocore_refuses() -> None:
    from botocore.exceptions import ParamValidationError

    with pytest.raises(ParamValidationError, match='Unknown parameter in input: "revision"'):
        ValidatingBatch().describe_job_definitions(jobDefinitionName="base-mnp-abc", revision=3)
    assert ValidatingBatch().describe_job_definitions(jobDefinitions=["base-mnp-abc:3"])["jobDefinitions"]


@pytest.mark.xfail(strict=True, reason="viva-api#730: describe_job_definitions has no `revision` parameter")
def test_the_per_node_vcpus_are_read_from_the_job_definition() -> None:
    service, fake = SimulationServiceRay(), ValidatingBatch()
    with (
        patch("viva_api.simulation.ray._seams.get_settings", _ray_settings),
        patch.object(service.batch, "client", return_value=fake),
        patch("viva_api.simulation.simulation_service_ray.time.sleep"),
    ):
        assert service._mnp_node_vcpus("base-mnp-abc:3") == 16
