from dataclasses import dataclass
from functools import cached_property

import pandas as pd

from oagdedupe.db.base import BaseCompute
from oagdedupe.db.postgres.blocking import PostgresBlocking
from oagdedupe.db.postgres.initialize import Initialize
from oagdedupe.db.postgres.orm import DatabaseORM


@dataclass
class PostgresCompute(BaseCompute, Initialize, DatabaseORM):
    """ """

    @cached_property
    def blocking(self):
        return PostgresBlocking(settings=self.settings)
