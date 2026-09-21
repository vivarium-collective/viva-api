"""Which image a compose run gets when it names one of SMS's simulators (``docs/plan-core.md`` P3d-3).

``ComposeSimulationRequest.simulator_id`` is an id in SMS's ``simulator`` table -- the same registry
``POST /api/v1/simulations`` uses, not compose's own. Compose declares the question
(``EnvironmentKeyOf``); this answers it, handed in by the composition root.
"""


async def simulator_environment_key(simulator_id: int) -> str:
    """The ``environment_key`` of simulator ``simulator_id``. ``ValueError`` when there is none."""
    from viva_api.dependencies import get_database_service

    database_service = get_database_service()
    if database_service is None:
        raise RuntimeError("Database service not initialized; cannot resolve simulator_id.")
    simulator = await database_service.get_simulator(simulator_id=simulator_id)
    if simulator is None:
        raise ValueError(f"Simulator {simulator_id} not found")
    return simulator.environment_key
