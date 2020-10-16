from sqlalchemy import create_engine
import pandas as pd
from urllib.request import urlopen
from zipfile import ZipFile
import os

thesauri_download_url = 'https://apps.usgs.gov/thesaurus/download/thesauri.zip'
term_base_url = "https://apps.usgs.gov/thesaurus/term.php"


def download_load_thesaurus_db():
    zipresp = urlopen(thesauri_download_url)
    tempzip = open("thesauri.zip", "wb")
    tempzip.write(zipresp.read())
    tempzip.close()
    zf = ZipFile("thesauri.zip")
    zf.extractall(path='./')
    zf.close()

    return create_engine('sqlite:///thesauri.db')


thesaurus_db = download_load_thesaurus_db()

df_terms = pd.read_sql("SELECT code, name, scope, IFNULL(parent, 0) AS parent FROM term", thesaurus_db)


def term_uri(term_code):
    return f"{term_base_url}?code={term_code}"


df_terms["term_uri"] = df_terms.apply(lambda row: term_uri(row["code"]), axis=1)

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

df_terms.to_sql(
    "usgs_thesaurus_terms",
    pg_engine,
    index=False,
    if_exists="replace",
    chunksize=1000
)
