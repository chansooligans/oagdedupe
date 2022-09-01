# %%
from dedupe.settings import Settings, SettingsOther
from dedupe.api import Dedupe

# from dedupe.api import RecordLinkage
from dedupe.datasets.fake import df
from pytest import fixture


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
