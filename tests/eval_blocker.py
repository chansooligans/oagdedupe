# %%
from dedupe.block.blockers import TestBlocker 
from dedupe.datasets.fake import df, df2
import pandas as pd
import pytest

# %%
@pytest.fixture()
def input_df():
    return pd.DataFrame({
        "name":["John Jacob", "John Jakob", "John Jakob"],
        "addr":["213 Avenue", "123 Avenue", "123 Ave"],
    })

def test_TestBlocker(input_df):
    blocker = TestBlocker()
    block_maps = blocker.get_block_maps(input_df, input_df.columns)
    assert len(blocker.dedupe_get_candidates(block_maps)) == 3


# %%
