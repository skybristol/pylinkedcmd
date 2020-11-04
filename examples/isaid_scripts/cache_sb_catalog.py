import sciencebasepy
import pylinkedcmd
from joblib import Parallel, delayed
import tqdm
from sqlalchemy import create_engine
import pandas as pd
import os
from copy import deepcopy

cmd_sb = pylinkedcmd.pylinkedcmd.Sciencebase()
sb = sciencebasepy.SbSession()

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

# Set classifiers for items used in our iSAID case
data_classifier = "USGS Data Release"
model_classifier = "USGS Model Catalog Item"

# Set up filter for only processing items processed since the last time this was run
end_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
date_filter_data_release = None
date_filter_models = None

# Set up stub for ScienceBase search criteria
search_criteria = {
    'fields': 'title,body,dates,contacts,tags,identifiers,distributionLinks',
    'max': 1000
}

# Get the last modified dates for records in the iSAID cache
last_record_data_release = pd.read_sql_query(
    f"SELECT max(datemodified) AS lastrecord FROM assets WHERE additionaltype = '{data_classifier}'",
    pg_engine
)["lastrecord"][0]

if last_record_data_release is not None:
    date_filter_data_release = {
        "dateType": "lastUpdated",
        "choice": "range",
        "start": datetime.strftime(dateutil.parser.parse(last_record_data_release), '%Y-%m-%d'),
        "end": end_date
    }

last_record_models = pd.read_sql_query(
    f"SELECT max(datemodified) AS lastrecord FROM assets WHERE additionaltype = '{model_classifier}'",
    pg_engine
)["lastrecord"][0]

if last_record_data_release is not None:
    data_filter_models = {
        "dateType": "lastUpdated",
        "choice": "range",
        "start": datetime.strftime(dateutil.parser.parse(last_record_data_release), '%Y-%m-%d'),
        "end": end_date
    }

# Run search for model catalog items and add iSAID classification
search_criteria_model_catalog = deepcopy(search_criteria)
search_criteria_model_catalog["parentId"] = "5ed7d36182ce7e579c66e3be"
if date_filter_models is not None:
    search_criteria_model_catalog["filter"] = str(date_filter_models)

model_catalog_items = list()

items = sb.find_items(search_criteria_model_catalog)
while items and 'items' in items:
    model_catalog_items.extend(items["items"])
    items = sb.next(items)

classified_model_catalog_items = [dict(item, **{'isaid_type': model_classifier}) for item in model_catalog_items]

# Run search for data release items and add iSAID classification
search_criteria_data_releases = deepcopy(search_criteria)
search_criteria_data_releases["filter0"] = "systemType=Data Release"
if date_filter_data_release is not None:
    search_criteria_data_releases["filter1"] = str(date_filter_models)

data_release_items = list()

items = sb.find_items(search_criteria_data_releases)
while items and 'items' in items:
    data_release_items.extend(items["items"])
    items = sb.next(items)

classified_data_release_items = [dict(item, **{'isaid_type': data_classifier}) for item in data_release_items]

# Put the lists together for processing
classified_data_release_items.extend(classified_model_catalog_items)

# Run summarization process that parses out the claims, links, and other bits from the data
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
        sb_item
    ) for sb_item in tqdm.tqdm(classified_data_release_items)
)

# Run processing to remove previous records tied to the identifiers processed here and add in new information
for k, v in summarization.items():
    if len(v) > 0:
        urls_for_query = str(
            list(set([
                i["url"] for i in v
            ]))).replace('"', "'").replace("[", "(").replace("]", ")")

        with pg_engine.connect() as con:
            rs = con.execute(f"DELETE FROM {k} WHERE url in {urls_for_query}")

        pd.DataFrame(v).to_sql(
            k,
            pg_engine,
            index=False,
            if_exists="append",
            chunksize=1000
        )
