from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
from pydantic import BaseModel
from sqlalchemy import engine, orm, sql

SESSION = orm.session.Session
TABLE = orm.decl_api.DeclarativeMeta
SUBQUERY = sql.selectable.Subquery
ENGINE = engine.base.Engine


# Fast API
class Dists(BaseModel):
    dists: List[List[float]]


# LSAPI
class Annotation(BaseModel):
    id: int
    created_username: str
    created_ago: str
    result: List[dict]
    was_cancelled: bool
    ground_truth: bool
    created_at: str
    updated_at: str
    lead_time: float
    task: int
    completed_by: int

    @property
    def label_map(self):
        return {"Match": 1, "Not a Match": 0, "Uncertain": 2}

    @property
    def label(self):
        if self.result:
            annotation = self.result[0]["value"]["choices"][0]
        else:
            annotation = "Uncertain"
        return self.label_map[annotation]


class Task(BaseModel):
    id: int
    cancelled_annotations: int
    total_annotations: int
    total_predictions: int
    updated_by: List[dict]
    data: dict
    created_at: str
    updated_at: str
    is_labeled: bool
    overlap: int
    project: int


class TaskList(BaseModel):
    total_annotations: int
    total: int
    tasks: List[Task]

    @property
    def n_incomplete(self):
        return self.total - self.total_annotations


class Project(BaseModel):
    id: int
    title: str
    description: Optional[str]
    label_config: str
    created_at: str
    created_by: dict
    num_tasks_with_annotations: Optional[int]
    task_number: Optional[int]


@dataclass
class StatsDict:
    n_pairs: int
    positives: int
    negatives: int
    conjunction: Tuple[str]
    rr: float

    def __hash__(self):
        return hash(self.conjunction)
