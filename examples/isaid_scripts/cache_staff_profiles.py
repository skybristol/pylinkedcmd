import pylinkedcmd
import pandas as pd
from sqlalchemy import create_engine
import os
from joblib import Parallel, delayed
import tqdm

usgs_web = pylinkedcmd.pylinkedcmd.UsgsWeb()

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

profile_urls = pd.read_sql(
    "SELECT profile FROM usgs_profile_inventory", pg_engine
)["profile"].to_list()

profiles = list()
claims = list()


def accumulator(url):
    profile_info = usgs_web.scrape_profile(url)
    profiles.append(profile_info["summary"])
    claims.extend(profile_info["claims"])


Parallel(n_jobs=20, prefer="threads")(
    delayed(accumulator)
    (
        i
    ) for i in tqdm.tqdm(profile_urls)
)

if len(profiles) == 0:
    print("NO PROFILES")
else:
    df_profiles = pd.DataFrame(profiles)
    del df_profiles["body_content_links"]
    del df_profiles["expertise"]
    df_profiles.to_sql(
        "usgs_profiles",
        pg_engine,
        index=False,
        if_exists="replace",
        chunksize=1000
    )

    pd.DataFrame(claims).to_sql(
        "claims",
        pg_engine,
        index=False,
        if_exists="append",
        chunksize=1000
    )
