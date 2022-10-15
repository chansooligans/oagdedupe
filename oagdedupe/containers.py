from dependency_injector import containers, providers

from oagdedupe.db.base import BaseCompute, BaseComputeBlocking
from oagdedupe.settings import Settings


class Container(containers.DeclarativeContainer):

    settings = providers.Factory(Settings)

    compute = providers.AbstractFactory(BaseCompute)

    blocking = providers.AbstractFactory(BaseComputeBlocking)


class SettingsContainer(containers.DeclarativeContainer):

    settings = providers.Factory(Settings)


class ComputeContainer(containers.DeclarativeContainer):

    compute = providers.AbstractFactory(BaseCompute)

    blocking = providers.AbstractFactory(BaseComputeBlocking)
