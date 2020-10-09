import pylinkedcmd
import pandas as pd
from sqlalchemy import create_engine
import pickle
import os

cmd_sb = pylinkedcmd.pylinkedcmd.Sciencebase()

if os.path.exists("raw_usgs_staff.p"):
    raw_usgs_staff = pickle.load(open("raw_usgs_staff.p", "rb"))
else:
    raw_usgs_staff = cmd_sb.get_active_usgs_staff(return_format="raw")
    pickle.dump(raw_usgs_staff, open("raw_usgs_staff.p", "wb"))

summarized_usgs_staff = [cmd_sb.summarize_sb_person(i) for i in raw_usgs_staff]

unique_staff = [
    next(i for i in summarized_usgs_staff if i["identifier_sb_uri"] == uri)
    for uri in list(set([i["identifier_sb_uri"] for i in summarized_usgs_staff]))
]

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

df_active_staff = pd.DataFrame(unique_staff)

df_active_staff.groupby('identifier_sb_uri', as_index=False).max().to_sql(
    "sb_usgs_staff",
    pg_engine,
    index=False,
    if_exists="replace",
    chunksize=1000
)
