from oagdedupe.db.postgres.compute import PostgresCompute
from oagdedupe.settings import Settings


def get_computer(settings: Settings):
    if settings.db:
        if settings.db.db == "postgresql":
            return PostgresCompute(settings=settings)
