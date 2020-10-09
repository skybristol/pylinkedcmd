import pylinkedcmd
from sqlalchemy import create_engine
import pandas as pd
from joblib import Parallel, delayed
import tqdm
import os

cmd_wd = pylinkedcmd.pylinkedcmd.Wikidata()

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

wikidata_ids = pd.read_sql_query(
    'SELECT identifier_wikidata FROM sb_usgs_staff WHERE identifier_wikidata IS NOT NULL',
    pg_engine
).identifier_wikidata.to_list()

wikidata_entities = list()
wikidata_claims = list()


def accumulator_wikidata(qid):
    entity_data, entity_claims = cmd_wd.entity_data(qid)
    if entity_data is not None:
        wikidata_entities.append(entity_data)
        if entity_claims is not None:
            wikidata_claims.extend(entity_claims)


Parallel(n_jobs=50, prefer="threads")(
    delayed(accumulator_wikidata)
    (
        url
    ) for url in tqdm.tqdm(wikidata_ids)
)

with pg_engine.connect() as con:
    con.execute("DELETE FROM wikidata_entities")
    con.execute("DELETE FROM wikidata_claims")
    con.execute("DELETE FROM wikidata_properties")
#    con.execute("DELETE FROM wikidata_claims_entities")
    con.close()

pd.DataFrame(wikidata_entities).to_sql(
    "wikidata_entities",
    pg_engine,
    index=False,
    if_exists="append",
    chunksize=1000
)

pd.DataFrame(wikidata_claims).to_sql(
    "wikidata_claims",
    pg_engine,
    index=False,
    if_exists="append",
    chunksize=1000
)

unique_wd_props = list(set([i["property_id"] for i in wikidata_claims]))

prop_info = list()


def accumulator_props(qid):
    entity_data, entity_claims = cmd_wd.entity_data(qid, claims=False)
    prop_info.append(entity_data)


Parallel(n_jobs=50, prefer="threads")(
    delayed(accumulator_props)
    (
        url
    ) for url in tqdm.tqdm(unique_wd_props)
)

pd.DataFrame(prop_info).to_sql(
    "wikidata_properties",
    pg_engine,
    index=False,
    if_exists="append",
    chunksize=1000
)

referenced_entities = list(set([i["property_value"] for i in wikidata_claims if i["property_value"].startswith("Q")]))

claim_refs_entities = list()


def accumulator_claim_refs(qid):
    entity_data, entity_claims = cmd_wd.entity_data(qid, claims=False)
    if entity_data is not None:
        claim_refs_entities.append(entity_data)


Parallel(n_jobs=25, prefer="threads")(
    delayed(accumulator_claim_refs)
    (
        url
    ) for url in tqdm.tqdm(referenced_entities)
)

pd.DataFrame(claim_refs_entities).to_sql(
    "wikidata_claim_entities",
    pg_engine,
    index=False,
    if_exists="append",
    chunksize=1000
)
