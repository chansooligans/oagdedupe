from oagdedupe.db.postgres.repository import PostgresCompute
from oagdedupe.settings import Settings


def get_computer(settings: Settings):
    if settings.db:
        if settings.db.db == "postgresql":
            return PostgresCompute(settings=settings)
