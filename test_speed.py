import numpy as np
import pandas as pd

df = pd.read_csv("./data/index.csv")
df.set_index(['table_schema','table_name'], drop=False, inplace=True)
print(df)
ans = df.loc['test_ef_db', 'tasks2133.0']
print(ans)
