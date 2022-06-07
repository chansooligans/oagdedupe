from faker import Faker
import pandas as pd

fake = Faker()
fake.seed_instance(0)
df = pd.DataFrame({
    'name':[fake.name() for x in range(100)],
    'addr':[fake.address() for x in range(100)]
})

attributes = ["name", "addr"]

df = pd.concat([
    df,
    df.assign(name=df["name"]+"x", addr=df["addr"]+"x")
], axis=0).reset_index(drop=True)

df2 = df.copy()