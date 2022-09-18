# %%
from dedupe.settings import Settings, SettingsOther
from dedupe.api import Dedupe

# from dedupe.api import RecordLinkage
from pytest import fixture

from faker import Faker
import pandas as pd
fake = Faker()
fake.seed_instance(0)
df = pd.DataFrame({
    'name': [fake.name() for x in range(100)],
    'addr': [fake.address() for x in range(100)]
})
# attributes = ["name", "addr"]
# df = pd.concat([
#     df,
#     df.assign(name=df["name"] + "x", addr=df["addr"] + "x")
# ], axis=0).reset_index(drop=True)
# df2 = df.copy()

@fixture
def d(tmp_path) -> Dedupe:
    d = Dedupe(
        df=df,
        settings=Settings(
            name="test_pipeline",
            folder=tmp_path,
            other=SettingsOther(
                attributes=["name", "addr"], path_database=tmp_path / "test_pipeline.db"
            ),
        ),
    )
    d.train()
    return d


# def test_pipeline_dedupe() -> None:
#     assert len(d.predict())==218
#
# def test_pipeline_rl() -> None:
#     predsx, predsy = rl.predict()
#     assert len(predsx)==len(predsy)==200


def test_get_candidates(d: Dedupe) -> None:
    assert len([x for x in d._get_candidates()]) == 394


# def test_get_candidates_rl() -> None:
#     assert len([x for x in rl._get_candidates()])==968
