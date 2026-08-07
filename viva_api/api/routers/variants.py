import logging

from viva_api.common.gateway.utils import get_router_config

logger = logging.getLogger(__name__)

config = get_router_config(prefix="variants")
