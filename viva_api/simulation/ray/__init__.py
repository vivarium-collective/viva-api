"""The Ray / AWS Batch simulation service, being carved out of ``simulation_service_ray.py``.

That module grew into one class of 4,305 lines holding eleven concerns and four dispatch
mechanisms (``docs/plan-core.md`` P2). Its pieces land here, one concern per PR. So far this
package holds :mod:`._seams` -- the prerequisite that makes moving anything safe -- and the first
cut, :mod:`.config_interpretation`.
"""
