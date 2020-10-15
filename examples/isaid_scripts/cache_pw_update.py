import pylinkedcmd
import pandas as pd
from sqlalchemy import create_engine
from joblib import Parallel, delayed
import tqdm
from datetime import datetime
import dateutil

cmd_pw = pylinkedcmd.pylinkedcmd.Pw()

pw_api = "https://pubs.er.usgs.gov/pubs-services/publication/"

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

last_record = pd.read_sql_query(
    "SELECT max(datemodified) AS lastrecord FROM assets WHERE url LIKE '%pubs.er.usgs.gov%'",
    pg_engine
)["lastrecord"][0]
last_x_days = abs((datetime.now()-dateutil.parser.parse(last_record)).days)

summarization = {
    "assets": list(),
    "sentences": list(),
    "contacts": list(),
    "claims": list(),
    "links": list()
}


def accumulator(pw_doc):
    item_data = cmd_pw.summarize_pw_record(pw_doc, parse_sentences=True)
    for k, v in item_data.items():
        if isinstance(v, list):
            summarization[k].extend(v)
        else:
            summarization[k].append(v)


if last_x_days > 0:
    modified_records = cmd_pw.pw_modifications(last_x_days)

    if len(modified_records) > 0:
        Parallel(n_jobs=50, prefer="threads")(
            delayed(accumulator)
                (
                pw_item
            ) for pw_item in tqdm.tqdm(modified_records)
        )

        urls_for_query = str(
            [
                i["url"] for i in summarization["assets"]
            ]).replace('"', "'").replace("[", "(").replace("]", ")")

        delete_statements = [
            f"DELETE FROM assets WHERE url in {urls_for_query}",
            f"DELETE FROM claims WHERE reference in {urls_for_query}",
            f"DELETE FROM links WHERE url in {urls_for_query}",
            f"DELETE FROM sentences WHERE url in {urls_for_query}",
            f"DELETE FROM contacts WHERE url in {urls_for_query}",
        ]

        with pg_engine.connect() as con:
            for statement in delete_statements:
                rs = con.execute(statement)
                for row in rs:
                    print(row)

        for k, v in summarization.items():
            if len(v) > 0:
                pd.DataFrame(v).to_sql(
                    k,
                    pg_engine,
                    index=False,
                    if_exists="append",
                    chunksize=1000
                )
