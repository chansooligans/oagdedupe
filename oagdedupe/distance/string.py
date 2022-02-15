from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass

from jellyfish import jaro_winkler_similarity

from oagdedupe.base import BaseDistance
from oagdedupe.mixin import DistanceMixin

@dataclass
class AllJaro(BaseDistance, DistanceMixin):

    def distance(self,x,y):
        return jaro_winkler_similarity(x,y)

    def config(self):
        """
        returns dict mapping each column to distance calculation
        """
        return dict