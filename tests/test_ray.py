# %%
from dedupe.distance.string import AllJaro
import pytest


@pytest.fixture()
def pairs():
    return [
        ['Jorge Sullivan', 'Jennifer Summers']
        for _ in range(30_000)
    ]

def test_ray_AllJaro(pairs):
    assert len(AllJaro().distance(pairs)) == 30_000
