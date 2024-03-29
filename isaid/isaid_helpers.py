# Config and helpers
import re
import requests
import pickle
import os
from neo4j import GraphDatabase
from io import StringIO
from html.parser import HTMLParser
import pandas as pd
import meilisearch

cache_api_domain = os.environ["CHS_ISAID_API"]
cache_api_domain_aggs = os.environ["CHS_ISAID_API_AGGS"]
cache_api_path = "prod"

local_cache_path = os.environ["LOCAL_CACHE_PATH"]
local_cache_path_rel = "data/"

f_mas_n_regions = f"{local_cache_path_rel}usgs_missions_regions.csv"
f_usgs_centers = f"{local_cache_path_rel}usgs_cost_centers.csv"
f_cost_center_projects = f"{local_cache_path_rel}sipp_cost_center_projects.p"

f_raw_profiles = f"{local_cache_path_rel}process_usgs_profiles.p"
f_graphable_profiles = f"{local_cache_path_rel}graphable_table_profile_entities.csv"
f_graphable_profile_expertise = f"{local_cache_path_rel}graphable_table_profile_expertise.csv"
f_graphable_profile_creative_works = f"{local_cache_path_rel}graphable_table_profile_creative_works.csv"

f_raw_sb_people = f"{local_cache_path_rel}process_sb_people.p"
f_graphable_sb_people = f"{local_cache_path_rel}graphable_table_sb_people.csv"

f_raw_orcid = f"{local_cache_path_rel}process_orcid.p"
f_graphable_orcid = f"{local_cache_path_rel}graphable_table_orcid.csv"

f_process_pw = f"{local_cache_path_rel}process_pw.p"
f_graphable_pw = f"{local_cache_path_rel}graphable_pw.csv"

f_raw_sdc = f"{local_cache_path_rel}process_sdc.p"
f_graphable_sdc = f"{local_cache_path_rel}graphable_table_sdc.csv"
f_graphable_sdc_rels_usgs_thesaurus = f"{local_cache_path_rel}graphable_table_sdc_usgs_thesaurus.csv"
f_graphable_sdc_rels_places = f"{local_cache_path_rel}graphable_table_sdc_places.csv"
f_graphable_sdc_rels_poc = f"{local_cache_path_rel}graphable_table_sdc_poc.csv"
f_graphable_sdc_rels_md = f"{local_cache_path_rel}graphable_table_sdc_md.csv"
f_graphable_sdc_rels_author = f"{local_cache_path_rel}graphable_table_sdc_author.csv"

f_common_geo_areas = f"{local_cache_path_rel}CommonGeographicAreas.db"
f_usgs_thesaurus_source = f"{local_cache_path_rel}thesauri.zip"
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

f_wd_reference = f"{local_cache_path_rel}wd_reference.p"
f_ner_reference = f"{local_cache_path_rel}ner_reference.p"

api_sipp_center_info = os.environ["SIPP_CENTERS"]
api_sipp_project_listing = os.environ["SIPP_PROJECT_COST_MASTER"]
api_sipp_project_narratives = os.environ["SIPP_PROJECT_NARRATIVES"]
api_sipp_project_task_details = os.environ["SIPP_PROJECT_TASK_DETAILS"]
api_sipp_project_staffing = os.environ["SIPP_PROJECT_STAFFING"]
api_sipp_account_detail = os.environ["SIPP_ACCOUNT_DETAIL"]

local_cache_path_sipp = f"{local_cache_path_rel}sipp/"
f_raw_sipp_project_listing = f"{local_cache_path_sipp}raw_ProjectTaskMaster.p"
f_raw_sipp_project_task_narrative = f"{local_cache_path_sipp}raw_ProjectTaskXML.p"
f_raw_sipp_project_staffing = f"{local_cache_path_sipp}raw_StaffRequestDetail.p"

f_graphable_sipp_personnel = f"{local_cache_path_rel}graphable_sipp_personnel.csv"
f_graphable_sipp_projects = f"{local_cache_path_rel}graphable_sipp_projects.csv"
f_graphable_sipp_staffing = f"{local_cache_path_rel}graphable_sipp_staffing.csv"

f_graphable_reference_terms = f"{local_cache_path_rel}graphable_reference_terms.csv"

graph_driver = GraphDatabase.driver(
    os.environ["NEO4J_CONX"],
    auth=(
        os.environ["NEO4J_USER"], 
        os.environ["NEO4J_PASS"]
    ),
)

graphdb = "isaid"

def get_facet_doc(facet_name):
    file_name = f"{local_cache_path_rel}facet_docs_{facet_name}.md"
    if os.path.exists(file_name):
        with open(file_name, "r") as f:
            return f.read()
    else:
        return

def get_search_client():
    search_client = meilisearch.Client(
        os.environ["SEARCH_CLIENT"],
        os.environ["SEARCH_CLIENT_KEY"]
    )
    return search_client

def cache_chs_cache(cache, exclude_errors=True):
    all_data = list()

    limit = 400
    api_path = f"{cache_api_domain}{cache_api_path}?es_search_index=cache_{cache}&page_size={limit}&scan=True"
    results = None
    while True:
        if results and "_scroll_id" in results and "scroll_id" not in api_path:
            api_path = f"{api_path}&scroll_id={results['_scroll_id']}"

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

def reference_terms_to_graph():
    if not os.path.exists(f_graphable_reference_terms):
        return

    with graph_driver.session(database=graphdb) as session:
        session.run("""
        LOAD CSV WITH HEADERS FROM '%(source_path)s/%(source_file)s' AS row
        WITH row
            MERGE (ds:DefinedSubjectMatter {url: row.url}) 
            ON CREATE
                SET ds.name = row.label,
                ds.description = row.description,
                ds.source = row.source,
                ds.reference = row.source_reference,
                ds.concept_label = row.concept_label
        """ % {
            "source_path": local_cache_path,
            "source_file": f_graphable_reference_terms
        })
        
        ds_in_graph = session.run("""
        MATCH (ds:DefinedSubjectMatter)
        RETURN ds.name AS name, ds.url AS url
        """).data()
        
    load_file_terms = pd.read_csv(f_graphable_reference_terms).to_dict(orient="records")
    return [i for i in load_file_terms if i["url"] not in [ds["url"] for ds in ds_in_graph]]
    
    
class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.text = StringIO()
    def handle_data(self, d):
        self.text.write(d)
    def get_data(self):
        return self.text.getvalue()

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

