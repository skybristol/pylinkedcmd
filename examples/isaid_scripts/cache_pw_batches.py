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
        ) for i in tqdm.tqdm([year for year in range(1867,2021)])
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


pw_summarization = {
    "pw_summaries": list(),
    "pw_sentences": list(),
    "pw_cost_centers": list(),
    "pw_authors": list(),
    "pw_affiliations": list(),
    "pw_links": list(),
    "pw_authors_to_cost_centers": list(),
    "pw_authors_to_affiliations": list(),
    "pw_authors_to_coauthors": list()
}


def process_pw_batch(url):
    pw_records = pw_file_cache(url)
    if pw_records is None:
        return None

    for record in pw_records:
        summary = cmd_pw.summarize_pw_record(record)

        pw_summarization["pw_summaries"].append(summary["summarized_record"])
        pw_summarization["pw_sentences"].extend(summary["record_sentences"])
        if summary["cost_centers"] is not None:
            pw_summarization["pw_cost_centers"].extend(summary["cost_centers"])
        if summary["authors"] is not None:
            pw_summarization["pw_authors"].extend(summary["authors"])
        if summary["affiliations"] is not None:
            pw_summarization["pw_affiliations"].extend(summary["affiliations"])
        if summary["links"] is not None:
            pw_summarization["pw_links"].extend(summary["links"])
        if summary["authors_to_cost_centers"] is not None:
            pw_summarization["pw_authors_to_cost_centers"].extend(summary["authors_to_cost_centers"])
        if summary["authors_to_affiliations"] is not None:
            pw_summarization["pw_authors_to_affiliations"].extend(summary["authors_to_affiliations"])
        if summary["authors_to_coauthors"] is not None:
            pw_summarization["pw_authors_to_coauthors"].extend(summary["authors_to_coauthors"])


Parallel(n_jobs=20, prefer="threads")(
    delayed(process_pw_batch)
    (
        i
    ) for i in tqdm.tqdm([i["url"] for i in pw_query_urls if 2019 <= i["year"] < 2021])
)

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

for k, v in pw_summarization.items():
    if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
        df = pd.DataFrame(v)
        try:
            df.to_sql(
                k,
                pg_engine,
                index=False,
                if_exists="append",
                chunksize=1000
            )
        except:
            existing_data = pd.read_sql(f"SELECT * FROM {k}", pg_engine)
            df_all = pd.concat([existing_data, df])
            df.to_sql(
                k,
                pg_engine,
                index=False,
                if_exists="replace",
                chunksize=1000
            )
