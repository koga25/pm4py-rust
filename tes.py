import pandas as pd
import pm4py
from time import time
#dtypes = [pl.Utf8, pl.Utf8, pl.Datetime("us"), pl.Datetime("us"), pl.Utf8, pl.Utf8, pl.Int64, pl.Int64, pl.Float64, pl.Utf8, pl.Utf8, pl.Utf8]
df = pd.read_csv("e:/rust/testing/src/20321.csv",)
#df = pd.read_csv("e:/rust/rust-pm4py/example.csv",)
df["start_timestamp"] = pd.to_datetime(df["start_timestamp"],)
df["time:timestamp"] = pd.to_datetime(df["time:timestamp"], )
t = time()
log = pm4py.convert_to_event_log(df)
dfg, start_activities, end_activities = pm4py.discover_dfg(log)
#
pm4py.save_vis_dfg(dfg, start_activities, end_activities, "dfg.svg")
#print(time() - t)



