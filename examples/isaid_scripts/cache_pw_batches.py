import pylinkedcmd
import pandas as pd
from sqlalchemy import create_engine
import os
from joblib import Parallel, delayed
import tqdm
import pickle
import uuid

cmd_pw = pylinkedcmd.pylinkedcmd.Pw()

pw_query_urls_file = "pw_cache/pw_query_urls.p"

if os.path.exists(pw_query_urls_file):
    pw_query_urls = pickle.load(open(pw_query_urls_file, "rb"))
else:
    def pw_query_accumulator(year):
        pw_query_urls.extend(cmd_pw.get_pw_query_urls(year))

    pw_query_urls = list()

    Parallel(n_jobs=10, prefer="threads")(
        delayed(pw_query_accumulator)
        (
            i
        ) for i in tqdm.tqdm([year for year in range(1867, 2021)])
    )

    pickle.dump(pw_query_urls, open(pw_query_urls_file, "wb"))


def pw_file_cache(url):
    file_name = f'pw_cache/{str(uuid.uuid3(uuid.NAMESPACE_URL, url))}'
    if os.path.exists(file_name):
        return pickle.load(open(file_name, "rb"))
    else:
        r = requests.get(url).json()
        if len(r["records"]) > 0:
            pw_records = [dict(item, **{'batch_url':url}) for item in r["records"]]
            pickle.dump(pw_records, open(file_name, "wb"))


summarization = {
    "assets": list(),
    "sentences": list(),
    "claims": list(),
    "lookups": list(),
    "links": list()
}


def process_pw_batch(url):
    pw_records = pw_file_cache(url)
    if pw_records is None:
        return None

    for record in pw_records:
        summary = cmd_pw.summarize_pw_record(record)

        summarization["assets"].append(summary["summary"])
        summarization["sentences"].extend(summary["sentences"])
        summarization["lookups"].extend(summary["lookup"])
        summarization["claims"].extend(summary["claims"])
        summarization["links"].extend(summary["links"])


Parallel(n_jobs=20, prefer="threads")(
    delayed(process_pw_batch)
    (
        i
    ) for i in tqdm.tqdm([i["url"] for i in pw_query_urls if i["year"] < 1900])
#    ) for i in tqdm.tqdm([i["url"] for i in pw_query_urls if 2019 <= i["year"] < 2021])
)

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

pd.DataFrame(pw_summarization["pw_summaries"]).to_sql(
    "assets",
    pg_engine,
    index=False,
    if_exists="append",
    chunksize=1000
)

for k, v in summarization.items():
    if len(v) > 0:
        pd.DataFrame(v).to_sql(
            k,
            pg_engine,
            index=False,
            if_exists="append",
            chunksize=1000
        )
