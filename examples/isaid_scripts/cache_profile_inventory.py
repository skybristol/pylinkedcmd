import pylinkedcmd
import pandas as pd
from sqlalchemy import create_engine
import os
from joblib import Parallel, delayed
import tqdm
import requests

usgs_web = pylinkedcmd.pylinkedcmd.UsgsWeb()

inventory_urls = usgs_web.get_staff_inventory_pages()

staff_inventory = list()


def accumulator(url):
    staff_list = usgs_web.get_staff_listing(url)
    if isinstance(staff_list, list):
        staff_inventory.extend(staff_list)
    else:
        print(type(staff_list))


Parallel(n_jobs=20, prefer="threads")(
    delayed(accumulator)
    (
        i
    ) for i in tqdm.tqdm(inventory_urls)
)

identifier_profiles = [
    {
        "identifier": identifier,
        "profile_list": [i["profile"] for i in staff_inventory if i["identifier"] == identifier]
    } for identifier in (list(set([x["identifier"] for x in staff_inventory])))
]

clean_inventory = [
    i for i in staff_inventory
    if i["identifier"] in
    [x["identifier"] for x in identifier_profiles if len(x["profile_list"]) == 1]
]

check_profiles = list()
for item in [i for i in identifier_profiles if len(i["profile_list"]) > 1]:
    for profile_url in item["profile_list"]:
        check_profiles.append({
            "identifier": item["identifier"],
            "profile": profile_url
        })


def url_length_accumulator(item):
    r = requests.get(item["profile"])
    profile_lengths.append({
        "identifier": item["identifier"],
        "profile": item["profile"],
        "content_length": len(str(r.text))
    })


profile_lengths = list()

Parallel(n_jobs=20, prefer="threads")(
    delayed(url_length_accumulator)
    (
        i
    ) for i in tqdm.tqdm(check_profiles)
)

best_profiles = list()
for identifier in list(set([i["identifier"] for i in profile_lengths])):
    identifier_profiles = [i for i in profile_lengths if i["identifier"] == identifier]
    max_length = max([i["content_length"] for i in identifier_profiles])
    best_profile = next(i["profile"] for i in identifier_profiles if i["content_length"] == max_length)
    best_inventory_item = next(
        i for i in staff_inventory if i["identifier"] == identifier and i["profile"] == best_profile
    )
    best_profiles.append(best_inventory_item)

clean_inventory.extend(best_profiles)

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

df_profiles = pd.DataFrame(clean_inventory)

df_profiles.to_sql(
    "usgs_profile_inventory",
    pg_engine,
    index=False,
    if_exists="replace",
    chunksize=1000
)

