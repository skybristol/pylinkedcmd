import pylinkedcmd
import pandas as pd
from sqlalchemy import create_engine
import os
from joblib import Parallel, delayed
import tqdm

usgs_web = pylinkedcmd.pylinkedcmd.UsgsWeb()

pg_user = os.environ["PG_USER"]
pg_pass = os.environ["PG_PASS"]
pg_host = os.environ["PG_HOST"]
pg_port = os.environ["PG_PORT"]
pg_db = os.environ["PG_DB"]

pg_engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

profile_urls = pd.read_sql(
    "SELECT profile FROM usgs_profile_inventory", pg_engine
)["profile"].to_list()

profiles = list()


def accumulator(url):
    profiles.append(usgs_web.scrape_profile(url))


Parallel(n_jobs=20, prefer="threads")(
    delayed(accumulator)
    (
        i
    ) for i in tqdm.tqdm(profile_urls)
)

if len(profiles) == 0:
    print("NO PROFILES")
else:
    df_profiles = pd.DataFrame(profiles)
    del df_profiles["body_content_links"]
    del df_profiles["expertise"]
    df_profiles.to_sql(
        "usgs_profiles",
        pg_engine,
        index=False,
        if_exists="replace",
        chunksize=1000
    )

    expertise_terms = list()
    for item in [i for i in profiles if "expertise" in i.keys() and len(i["expertise"]) > 0]:
        for term in item["expertise"]:
            for t in term.split(","):
                d_term = {
                    "term_source": "USGS Staff Profiles",
                    "source_identifier": item["profile"],
                    "term": t.strip()
                }
                expertise_terms.append(d_term)

    if len(expertise_terms) > 0:
        df_expertise = pd.DataFrame(expertise_terms)
        df_expertise.to_sql(
            "expertise_terms",
            pg_engine,
            index=False,
            if_exists="replace",
            chunksize=1000
        )

    profile_links = list()
    for item in [i for i in profiles if "links" in i.keys() and len(i["links"]) > 0]:
        for link in item["links"]:
            d_link = {
                "link_source": "USGS Staff Profiles",
                "source_identifier": item["profile"],
                "link_href": link
            }
            profile_links.append(d_link)

    if len(profile_links) > 0:
        df_profile_links = pd.DataFrame(profile_links)
        df_profile_links.to_sql(
            "profile_links",
            pg_engine,
            index=False,
            if_exists="replace",
            chunksize=1000
        )
