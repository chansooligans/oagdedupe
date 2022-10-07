"""This module contains mixins with common methods in oagdedupe.block
"""

import logging
from dataclasses import dataclass
from functools import cached_property, lru_cache
from multiprocessing import Pool
from typing import Dict, List, Optional, Protocol, Tuple

import tqdm

from oagdedupe._typing import ENGINE, StatsDict
from oagdedupe.base import BaseOptimizer
from oagdedupe.block.forward import Forward
from oagdedupe.block.sql import LearnerSql
from oagdedupe.settings import Settings


class ConjunctionMixin:
    def _max_key(self, x: StatsDict) -> Tuple[float, int, int]:
        """
        block scheme stats ordering
        """
        return (x.rr, x.positives, -x.negatives)
