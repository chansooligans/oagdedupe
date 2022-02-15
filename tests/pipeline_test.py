# %%
from oagdedupe.model import Dedupe, RecordLinkage
from oagdedupe.block.blockers import NoBlocker
from oagdedupe.datasets.fake import df, df2

import pandas as pd
import numpy as np

attributes = ["name", "addr"]
d = Dedupe(df=df, attributes=attributes)
rl = RecordLinkage(df=df, df2=df.copy(), attributes=attributes, attributes2=attributes)

def test_pipeline_dedupe() -> None:
    assert len(d.predict())==22

def test_pipeline_rl() -> None:
    predsx, predsy = rl.predict()
    assert len(predsx)==len(predsy)==20

def test_get_candidates() -> None:
    assert len([x for x in d._get_candidates()])==15

def test_get_candidates_rl() -> None:
    assert len([x for x in rl._get_candidates()])==48
