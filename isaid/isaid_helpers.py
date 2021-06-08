# Config and helpers
import re
import requests
import pickle
import os
from neo4j import GraphDatabase

cache_api_domain = os.environ["CHS_ISAID_API"]
cache_api_path = "prod"

local_cache_path = "file:////Users/sbristol/github_skybristol/pylinkedcmd/isaid/"
local_cache_path_rel = "data/"

f_mas_n_regions = f"{local_cache_path_rel}usgs_missions_regions.csv"
f_usgs_centers = f"{local_cache_path_rel}usgs_cost_centers.csv"

f_raw_profiles = f"{local_cache_path_rel}process_usgs_profiles.p"
f_graphable_profiles = f"{local_cache_path_rel}graphable_table_profile_entities.csv"
f_graphable_profile_expertise = f"{local_cache_path_rel}graphable_table_profile_expertise.csv"
f_graphable_profile_creative_works = f"{local_cache_path_rel}graphable_table_profile_creative_works.csv"

f_raw_sb_people = f"{local_cache_path_rel}process_sb_people.p"
f_graphable_sb_people = f"{local_cache_path_rel}graphable_table_sb_people.csv"

f_raw_orcid = f"{local_cache_path_rel}process_orcid.p"
f_graphable_orcid = f"{local_cache_path_rel}graphable_table_orcid.csv"

f_raw_sdc = f"{local_cache_path_rel}process_sdc.p"
f_graphable_sdc = f"{local_cache_path_rel}graphable_table_sdc.csv"
f_graphable_sdc_rels_usgs_thesaurus = f"{local_cache_path_rel}graphable_table_sdc_usgs_thesaurus.csv"
f_graphable_sdc_rels_places = f"{local_cache_path_rel}graphable_table_sdc_places.csv"
f_graphable_sdc_rels_poc = f"{local_cache_path_rel}graphable_table_sdc_poc.csv"
f_graphable_sdc_rels_md = f"{local_cache_path_rel}graphable_table_sdc_md.csv"
f_graphable_sdc_rels_author = f"{local_cache_path_rel}graphable_table_sdc_author.csv"

f_common_geo_areas = f"{local_cache_path_rel}CommonGeographicAreas.db"
f_usgs_thesaurus = f"{local_cache_path_rel}thesauri.db"

f_graphable_thesaurus_terms = f"{local_cache_path_rel}graphable_table_usable_usgs_thesaurus_terms.csv"
f_graphable_place_names = f"{local_cache_path_rel}graphable_table_usable_usgs_thesaurus_places.csv"

model_catalog_api = "https://www.sciencebase.gov/catalog/items?&max=200&folderId=5ed7d36182ce7e579c66e3be&format=json&fields=title,subTitle,summary,contacts,tags,webLinks,provenance,previewImage"
f_raw_model_catalog = f"{local_cache_path_rel}usgs_model_catalog.p"
f_graphable_model_catalog = f"{local_cache_path_rel}graphable_table_model_catalog.csv"

f_raw_doi = f"{local_cache_path_rel}doi.p"
f_graphable_doi = f"{local_cache_path_rel}graphable_table_doi.csv"
f_graphable_doi_contacts = f"{local_cache_path_rel}graphable_table_doi_contacts.csv"
f_graphable_doi_funders = f"{local_cache_path_rel}graphable_table_doi_funders.csv"
f_graphable_doi_terms = f"{local_cache_path_rel}graphable_table_doi_terms.csv"

center_info_link = 'https://sipp.cr.usgs.gov/SIPPService/CenterInfo.ashx'

graph_driver = GraphDatabase.driver(
    os.environ["NEO4J_CONX"],
    auth=(
        os.environ["NEO4J_USER"], 
        os.environ["NEO4J_PASS"]
    ),
)

graphdb = "isaid"

def cache_chs_cache(cache, exclude_errors=True):
    all_data = list()

    limit = 400
    page_num = 1
    while True:
        api_path = f"https://{cache_api_domain}/{cache_api_path}?es_search_index=cache_{cache}&page_size={limit}&page={page_num}"
        results = requests.get(api_path).json()

        try:
            if not results["hits"]["hits"]:
                break
            else:
                if exclude_errors:
                    docs = [i["_source"] for i in results["hits"]["hits"] if "error" not in i]
                else:
                    docs = [i["_source"] for i in results["hits"]["hits"]]
                all_data.extend(docs)
                page_num += 1
        except:
            print(api_path)
            print(results)
            break
            
    return all_data

def active_usgs_emails(raw_sb_directory_file=f_raw_sb_people):
    if not os.path.exists(raw_sb_directory_file):
        raise ValueError("The raw ScienceBase Directory cache file doesn't exist. Run that first.")
        
    sb_people = pickle.load(open(raw_sb_directory_file, "rb"))
    emails = list(set([
        p["email"] for p in sb_people
        if "email" in p and p["active"]
    ]))
    
    return emails
