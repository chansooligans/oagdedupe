from dataclasses import dataclass
from functools import cached_property

import pandas as pd
from sqlalchemy import create_engine

from oagdedupe.block.schemes import BlockSchemes
from oagdedupe.db.base import BaseRepository
from oagdedupe.db.postgres.blocking import PostgresBlocking
from oagdedupe.db.postgres.initialize import Initialize
from oagdedupe.db.postgres.orm import Cluster, Distance, Fapi
from oagdedupe.settings import Settings


@dataclass
class PostgresRepository(BaseRepository, Initialize, Distance, Cluster, Fapi):
    """concrete implementation for repository"""

    settings: Settings

    @cached_property
    def engine(self):
        """manages dbapi connection, created once"""
        return create_engine(self.settings.db.path_database)

    @cached_property
    def blocking(self):
        return PostgresBlocking(settings=self.settings)
