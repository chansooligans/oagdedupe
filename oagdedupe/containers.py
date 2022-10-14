from dependency_injector import containers, providers

from oagdedupe.base import BaseCompute
from oagdedupe.settings import Settings


class Container(containers.DeclarativeContainer):

    wiring_config = containers.WiringConfiguration(
        packages=[
            "oagdedupe.db",
            "oagdedupe.block",
            "oagdedupe.distance",
            "oagdedupe.cluster",
        ],
    )

    settings = providers.Factory(Settings)

    compute = providers.Factory(BaseCompute)
