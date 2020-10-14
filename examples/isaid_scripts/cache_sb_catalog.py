import sciencebasepy
import pylinkedcmd
from joblib import Parallel, delayed
import tqdm
from sqlalchemy import create_engine
import pandas as pd
import os

cmd_sb = pylinkedcmd.pylinkedcmd.Sciencebase()
sb = sciencebasepy.SbSession()

data_release_items = list()
items = sb.find_items(
    {
        'filter': 'systemType=Data Release',
        'fields': 'title,dates,contacts,tags,webLinks',
        'max': 1000
    }
)
while items and 'items' in items:
    data_release_items.extend(items["items"])
    items = sb.next(items)

summarization = {
    "assets": list(),
    "sentences": list(),
    "claims": list(),
    "lookups": list(),
    "links": list()
}


def accumulator(sb_doc):
    item_data = cmd_sb.sb_catalog_item_summary(sb_doc)
    summarization["assets"].append(item_data["summary"])
    summarization["sentences"].extend(item_data["sentences"])
    summarization["claims"].extend(item_data["claims"])
    summarization["lookups"].extend(item_data["lookups"])
    summarization["links"].extend(item_data["links"])


Parallel(n_jobs=50, prefer="threads")(
    delayed(accumulator)
    (
        pw_item
    ) for pw_item in tqdm.tqdm(data_release_items)
)

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

for k, v in summarization.items():
    if len(v) > 0:
        pd.DataFrame(v).to_sql(
            k,
            pg_engine,
            index=False,
            if_exists="append",
            chunksize=1000
        )
