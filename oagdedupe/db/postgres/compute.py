import itertools
import logging
from dataclasses import dataclass
from functools import cached_property
from typing import List

from dependency_injector.wiring import Provide
from sqlalchemy import delete, func, select

from oagdedupe import utils as du
from oagdedupe._typing import SESSION, TABLE
from oagdedupe.base import BaseCompute, BaseComputeBlocking
from oagdedupe.containers import Container
from oagdedupe.db.postgres.blocking import PostgresBlocking
from oagdedupe.db.postgres.initialize import Initialize
from oagdedupe.db.postgres.orm import DatabaseORM
from oagdedupe.db.postgres.tables import Tables
from oagdedupe.distance.string import AllJaro
from oagdedupe.settings import Settings


@dataclass
class PostgresCompute(BaseCompute, Initialize, DatabaseORM, Tables):
    """ """

    settings: Settings = Provide[Container.settings]

    @cached_property
    def blocking(self) -> BaseComputeBlocking:
        return PostgresBlocking()
