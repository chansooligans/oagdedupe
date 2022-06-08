# %%
from dedupe.block.blockers import TestBlocker 
from dedupe.datasets.fake import df, df2
print(df.head())


# %%
blocker = TestBlocker()
blocker.get_block_maps(df, df.columns)
# %%
