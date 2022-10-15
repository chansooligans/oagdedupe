from dependency_injector import containers, providers

from oagdedupe.settings import Settings


class SettingsContainer(containers.DeclarativeContainer):

    settings = providers.Factory(Settings)
