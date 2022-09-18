from dedupe.settings import Settings

from functools import cached_property
from sqlalchemy import create_engine

class Engine:
    settings:Settings

    @cached_property
    def engine(self):
        return create_engine(self.settings.other.path_database)