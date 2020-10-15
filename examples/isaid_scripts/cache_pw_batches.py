import pylinkedcmd
import pandas as pd
from sqlalchemy import create_engine
import os
from joblib import Parallel, delayed
import tqdm
import pickle
from os import listdir
from os.path import isfile, join

cmd_pw = pylinkedcmd.pylinkedcmd.Pw()

source_path = "pw_cache"
processed_path = "pw_cache_processed"

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


def accumulator(pw_doc):
    item_data = cmd_pw.summarize_pw_record(pw_doc, parse_sentences=True)
    for k, v in item_data.items():
        if isinstance(v, list):
            summarization[k].extend(v)
        else:
            summarization[k].append(v)


pw_cache_files = [
    f for f in listdir(source_path)
    if isfile(join(source_path, f))
]

while len(pw_cache_files) > 0:
    print(pw_cache_files[0])

    pw_records = pickle.load(open(f"{source_path}/{pw_cache_files[0]}", "rb"))

    summarization = {
        "assets": list(),
        "sentences": list(),
        "claims": list(),
        "lookups": list(),
        "links": list()
    }

    Parallel(n_jobs=50, prefer="threads")(
        delayed(accumulator)
            (
            pw_item
        ) for pw_item in tqdm.tqdm(pw_records)
    )

    for k, v in summarization.items():
        if len(v) > 0:
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

    os.rename(f"{source_path}/{pw_cache_files[0]}", f"{processed_path}/{pw_cache_files[0]}")

    pw_cache_files = [
        f for f in listdir(source_path)
        if isfile(join(source_path, f))
    ]
