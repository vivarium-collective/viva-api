"""Core's routers. Each is an ``APIRouter`` with no prefix of its own: ``create_core_app`` serves them
under ``/viva/v1/…``, and an application that embeds core may serve the same router at a prefix its
callers already use (SMS: ``/compose/v1``)."""
