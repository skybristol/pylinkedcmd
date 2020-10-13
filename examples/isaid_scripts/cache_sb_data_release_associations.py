import sciencebasepy
import pylinkedcmd
from joblib import Parallel, delayed
import tqdm
from sqlalchemy import create_engine
import pandas as pd

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

data_release_info = list()


def accumulator(sb_doc):
    data_release_info.extend(cmd_sb.sb_suggested_associations(sb_doc))


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

df_data_release_info = pd.DataFrame(data_release_info)

df_data_release_info.to_sql(
    "data_release_associations",
    pg_engine,
    index=False,
    if_exists="replace",
    chunksize=1000
)
