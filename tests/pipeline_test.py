# %%
from dedupe.api import Dedupe, RecordLinkage
from dedupe.block.blockers import NoBlocker
from dedupe.datasets.fake import df, df2

import pandas as pd
import numpy as np

d = Dedupe(df=df)
rl = RecordLinkage(df=df, df2=df.copy())

def test_pipeline_dedupe() -> None:
    assert len(d.predict())==22

def test_pipeline_rl() -> None:
    predsx, predsy = rl.predict()
    assert len(predsx)==len(predsy)==20

def test_get_candidates() -> None:
    assert len([x for x in d._get_candidates()])==15

def test_get_candidates_rl() -> None:
    assert len([x for x in rl._get_candidates()])==48

# %%
