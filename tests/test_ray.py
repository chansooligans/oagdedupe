# %%
from dedupe.distance.string import RayAllJaro
from dedupe.settings import Settings
import pytest
from pytest import fixture

@fixture
def settings(tmp_path) -> Settings:
    return Settings(
        name="test",
        folder=tmp_path
    )

@pytest.fixture()
def pairs():
    return [
        ['Jorge Sullivan', 'Jennifer Summers']
        for _ in range(1_000)
    ]

def test_ray_AllJaro(settings, pairs):
    assert len(RayAllJaro(settings).distance(pairs)) == 1_000
