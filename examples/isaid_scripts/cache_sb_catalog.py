import sciencebasepy
import pylinkedcmd
from joblib import Parallel, delayed
import tqdm
from sqlalchemy import create_engine
import pandas as pd
import os

cmd_sb = pylinkedcmd.pylinkedcmd.Sciencebase()

'''
sb = sciencebasepy.SbSession()

data_release_items = list()

items = sb.find_items(
    {
        'filter': 'systemType=Data Release',
        'fields': 'title,body,dates,contacts,tags,identifiers,distributionLinks',
        'max': 1000
    }
)
while items and 'items' in items:
    data_release_items.extend(items["items"])
    items = sb.next(items)
'''

import pickle
data_release_items = pickle.load(open("data_release_items.p", "rb"))

summarization = {
    "assets": list(),
    "sentences": list(),
    "contacts": list(),
    "claims": list(),
    "links": list()
}


def accumulator(sb_doc):
    item_data = cmd_sb.catalog_item_summary(sb_doc, parse_sentences=True)
    for k, v in item_data.items():
        if isinstance(v, list):
            summarization[k].extend(v)
        else:
            summarization[k].append(v)


Parallel(n_jobs=50, prefer="threads")(
    delayed(accumulator)
    (
        pw_item
    ) for pw_item in tqdm.tqdm(data_release_items)
)

#for k, v in summarization.items():
#    print(k, len(v))

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

for k, v in summarization.items():
    if len(v) > 0 and k in ["links"]:
        try:
            pd.DataFrame(v).to_sql(
                k,
                pg_engine,
                index=False,
                if_exists="append",
                chunksize=1000
            )
        except:
            new_data = pd.DataFrame(v)
            existing_data = pd.read_sql(f'SELECT * FROM {k}', pg_engine)
            df_combined = pd.concat([new_data, existing_data])
            df_combined.to_sql(
                k,
                pg_engine,
                index=False,
                if_exists="replace",
                chunksize=1000
            )
