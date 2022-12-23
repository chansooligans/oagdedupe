# %%
from fastapi import FastAPI
from uvicorn import run
from dataclasses import dataclass
from nest_asyncio import apply

def get_pairs_to_label():
    return {"message": "test2"}

def add_label():
    return {"message": "test2"}

@dataclass
class Learner:
    # records: DataFrame
    # attributes: Set[Attribute]
    # name_field_id: str
    # conj_finder: ConjunctionFinder
    # class_learner: ClassifierLearner

    def __post_init__(self):
        apply()
        run(self.app, host="0.0.0.0", port="8001")
    
    @property
    def app(self) -> FastAPI:
        a = FastAPI()
        a.get("/get")(root)
        return a

# %%
Learner()