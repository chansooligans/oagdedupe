from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass
from oagdedupe.base import BaseDistance
from jellyfish import jaro_winkler_similarity

@dataclass
class AllJaro(BaseDistance):

    def distance(self,x,y):
        return jaro_winkler_similarity(x,y)

    def config(self):
        """
        returns dict mapping each column to distance calculation
        """
        return dict