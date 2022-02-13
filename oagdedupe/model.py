from abc import ABCMeta, abstractmethod
from typing import List, Union, Any, Optional, Dict
from dataclasses import dataclass

class BaseModel(metaclass=ABCMeta):
    """ Abstract base class from which all model classes inherit.
    All descendent classes must implement predict, train, and candidates methods.
    """

    @abstractmethod
    def predict(
        self,
        df, 
        cols
    ):
    """
    Use trained model to identify matched candidates.
    Use clustering algorithm to assign cluster IDs. Default is to define each connected component as a cluster.
    Handle unclustered nodes.
    Returns cluster IDs
    """

    @abstractmethod    
    def train(

    ):
    """
    Computes similarity scores for each column.
    Then fits a model to learn p(match).
    Default model is to average similarity scores and use a threshold of 0.9.
    Return model that takes any dataframe or dataframes with same columns, and returns matched candidates.
    """

    @abstractmethod
    def get_candidates(
        self
    ):
    """
    Returns candidate pairs of records. Blocking is optional.
    """


@dataclass
class Dedupe(BaseModel):
    """General dedupe block, inherits from BaseModel.
    """


    def predict(
        self,
        df, 
        cols
    ):

    def train(
        self
    ):

    def get_candidates(
        self
    ):
    """
    Returns candidate pairs of records. Blocking is optional.
    """

@dataclass
class RecordLinkage(BaseModel):
    """General record linkage block, inherits from BaseModel.
    """

    def predict(
        self,
        df, 
        cols
    ):

    def train(

    ):

    def get_candidates(
        self
    ):
    """
    Returns candidate pairs of records. Blocking is optional.
    """
