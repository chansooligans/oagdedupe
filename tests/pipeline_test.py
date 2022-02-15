# %%
from oagdedupe.model import Dedupe, RecordLinkage
from oagdedupe.block.blockers import NoBlocker
import pandas as pd

import numpy as np
from faker import Faker

fake = Faker()
fake.seed_instance(0)

df = pd.DataFrame({
    'name':[fake.name() for x in range(10)],
    'addr':[fake.address() for x in range(10)]
})
df = pd.concat([
    df,
    df.assign(name=df["name"]+"x", addr=df["addr"]+"x")
], axis=0).reset_index(drop=True)

attributes = ["name", "addr"]

d = Dedupe(df=df, attributes=attributes)
rl = RecordLinkage(df=df, df2=df.copy(), attributes=attributes, attributes2=attributes)

def test_dedupe_pipeline() -> None:
    df_clusters = d.predict()
    assert len(df_clusters)==22

def test_get_candidates() -> None:
    assert len([x for x in d._get_candidates()])==15

def test_get_candidates_rl() -> None:
    assert len([x for x in rl._get_candidates()])==48
