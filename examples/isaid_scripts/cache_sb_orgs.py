import sciencebasepy
import pylinkedcmd
from joblib import Parallel, delayed
import tqdm
from sqlalchemy import create_engine
import pandas as pd
import os

cmd_sb = pylinkedcmd.pylinkedcmd.Sciencebase()
cmd_isaid = pylinkedcmd.pylinkedcmd.Isaid()

orgs = cmd_isaid.get_organizations()

org_summary = list()

def accumulator(identifier):
    org_summary.append(cmd_sb.get_sb_org(identifier))


Parallel(n_jobs=4, prefer="threads")(
    delayed(accumulator)
    (
        org["organization_uri"]
    ) for org in tqdm.tqdm(orgs)
)

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

pd.DataFrame(org_summary).to_sql(
    "organizations",
    pg_engine,
    index=False,
    if_exists="replace",
    chunksize=1000
)
