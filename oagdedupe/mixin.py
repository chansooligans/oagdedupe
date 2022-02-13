from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass
import itertools

class BlockerMixin:
    
    def product(self, lists, nodupes=True):
        result = [[]]
        for item in lists:
            if nodupes==True:
                result = [x+[y] for x in result for y in item if x != [y]]
            else:
                result = [x+[y] for x in result for y in item]
        return result