from oagdedupe.db.postgres.repository import PostgresRepository
from oagdedupe.settings import Settings


def get_repository(settings: Settings):
    if settings.db:
        if settings.db.db == "postgresql":
            return PostgresRepository(settings=settings)
