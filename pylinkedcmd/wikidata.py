import requests
import datetime
import time

wikidata_reference = [
    {
        "source_label": "Wikidata Mineral Species",
        "concept_label": "MINERAL_SPECIES",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q12089225",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Chemical Elements",
        "concept_label": "CHEMICAL_ELEMENT",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q11344",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Sedimentary Rocks",
        "concept_label": "SEDIMENTARY_ROCK",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q82480",
        "source_rel": "P279",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Clastic Sediments",
        "concept_label": "CLASTIC_SEDIMENT",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q12372934",
        "source_rel": "P279",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Sovereign States",
        "concept_label": "SOVEREIGN_STATE",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q3624078",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata US States",
        "concept_label": "US_STATE",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q35657",
        "source_rel": "P31",
        "include_alt_names": False
    },
    {
        "source_label": "Wikidata Global Seas and Oceans",
        "concept_label": "SEA_OR_OCEAN",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q165",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Global Faults",
        "concept_label": "GEOLOGIC_FAULT",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q47089",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Global Volcanos",
        "concept_label": "NAMED_VOLCANO",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q8072",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Global Earthquakes",
        "concept_label": "NAMED_EARTHQUAKE",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q7944",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata US National Parks",
        "concept_label": "NATIONAL_PARK",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q34918903",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata US National Monuments",
        "concept_label": "NATIONAL_MONUMENT",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q893775",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata US National Forests",
        "concept_label": "NATIONAL_FOREST",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q612741",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata US Wild and Scenic Rivers",
        "concept_label": "WILD_AND_SCENIC_RIVER",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q846385",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Geologic Formations",
        "concept_label": "GEOLOGIC_FORMATION",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q736917",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Aquifers",
        "concept_label": "NAMED_GROUNDWATER_AQUIFER",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q208791",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Fields of Science",
        "concept_label": "FIELD_OF_SCIENCE",
        "retrieval_type": "source relationship",
        "source_reference": "https://www.wikidata.org/wiki/Q2465832",
        "source_rel": "P31",
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata Additional Commodities",
        "concept_label": "GEOLOGIC_COMMODITY_OR_MATERIAL",
        "retrieval_type": "identifier list",
        "source_reference": "Wikidata specific identifiers",
        "identifier_list": [
            "Q83437",
            "Q223995",
            "Q10564271",
            "Q190444"
        ],
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata US Territories",
        "concept_label": "US_TERRITORY",
        "retrieval_type": "specialized query",
        "source_reference": "Wikidata US territories references",
        "query_criteria": '?item wdt:P31 wd:Q1352230. ?item wdt:P31 wd:Q462778.',
        "include_alt_names": True
    },
    {
        "source_label": "Wikidata US Counties",
        "concept_label": "US_COUNTY",
        "retrieval_type": "source relationship multi",
        "source_reference": "Wikidata county of state instances",
        "identifier_list": [
            'Q13410411',
            'Q13410522',
            'Q13410496',
            'Q13414369',
            'Q13414753',
            'Q13414757',
            'Q13414761',
            'Q11812346',
            'Q13212489',
            'Q13410433',
            'Q13410454',
            'Q13410485',
            'Q13410520',
            'Q13414754',
            'Q13414763',
            'Q12037308',
            'Q12178655',
            'Q13410400',
            'Q13410422',
            'Q13414764',
            'Q13414765',
            'Q12178928',
            'Q13217186',
            'Q13410403',
            'Q13410447',
            'Q13410444',
            'Q13410524',
            'Q13414760',
            'Q13415365',
            'Q11774062',
            'Q11774149',
            'Q13410438',
            'Q13410508',
            'Q13414358',
            'Q13415366',
            'Q13414755',
            'Q13414759',
            'Q13415369',
            'Q12262532',
            'Q13410431',
            'Q13410464',
            'Q13414354',
            'Q13414758',
            'Q13415370',
            'Q13415371',
            'Q11774097',
            'Q13410428',
            'Q13414361',
            'Q13415367',
            'Q13415368',
            'Q13410454'
        ],
        "source_rel": "P31",
        "include_alt_names": False
    }
]

def execute_wd_query(query, wd_api='https://query.wikidata.org/sparql'):
    results = requests.get(
        wd_api, 
        params = {'format': 'json', 'query': query}
    )

    if results.status_code == 200:
        return results.json()
    else:
        return results

def get_wd_concepts(wd_source, wd_reference=wikidata_reference, limit=10000):
    source_config = next((i for i in wd_reference if i["source_label"] == wd_source), None)
    if source_config is None:
        return list()

    wd_query_start = 'SELECT ?item ?itemLabel ?itemDescription ?itemAltLabel WHERE {SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }'
    wd_query_end = "} LIMIT 20000"

    if source_config["retrieval_type"] == "source relationship":
        wd_query_criteria = f'?item wdt:{source_config["source_rel"]} wd:{source_config["source_reference"].split("/")[-1]}.'

    elif source_config["retrieval_type"] == "identifier list":
        wd_query_criteria = 'VALUES ?item {' + " ".join([f"wd:{i}" for i in source_config["identifier_list"]]) + '}'

    elif source_config["retrieval_type"] == "specialized query":
        wd_query_criteria = source_config["query_criteria"]

    elif source_config["retrieval_type"] == "source relationship multi":
        wd_query_criteria = " UNION ".join(["{?item wdt:" + source_config["source_rel"] + " wd:" + i + "}" for i in source_config["identifier_list"]])

    wd_results = execute_wd_query(wd_query_start + wd_query_criteria + wd_query_end)

    concept_list = list()
    for i in wd_results["results"]["bindings"]:
        if i["itemLabel"]["value"] != i["item"]["value"].split("/")[-1]:
            concept = {
                "_date_cached": datetime.datetime.utcnow().isoformat(),
                "source": source_config["source_label"],
                "source_reference": source_config["source_reference"], 
                "label": i["itemLabel"]["value"],
                "concept_label": source_config["concept_label"],
                "identifier": i["item"]["value"],
                "label_source": "preferred"
            }
            if "itemDescription" in i:
                concept["description"] = i["itemDescription"]["value"]
            concept_list.append(concept)

    if source_config["include_alt_names"]:
        for item in [i for i in wd_results["results"]["bindings"] if "itemAltLabel" in i]:
            if item["itemLabel"]["value"] != item["item"]["value"].split("/")[-1]:
                for alt_label in item["itemAltLabel"]["value"].split(","):
                    if alt_label.strip() != item["itemLabel"]["value"]:
                        alt_concept = {
                                "_date_cached": datetime.datetime.utcnow().isoformat(),
                                "source": source_config["source_label"],
                                "source_reference": source_config["source_reference"], 
                                "label": alt_label.strip(),
                                "concept_label": source_config["concept_label"],
                                "label_preferred": item["itemLabel"]["value"],
                                "identifier": item["item"]["value"],
                                "label_source": "alternate"
                            }
                        if "itemDescription" in item:
                            alt_concept["description"] = item["itemDescription"]["value"]
                        concept_list.append(alt_concept)

    return concept_list

def build_wd_reference(
    sources=[i["source_label"] for i in wikidata_reference], 
    existing_data=None, 
    include_uncertainty=True):
    
    wd_reference = list()
    if existing_data is not None:
        wd_reference = existing_data
        existing_sources = list(set([i["source"] for i in existing_data]))
        sources = [i for i in sources if i not in existing_sources]

    for source in sources:
        wd_reference.extend(get_wd_concepts(source))
        time.sleep(5)

    if len(sources) > 0 and include_uncertainty:
        wd_reference = wd_reference_uncertainty_factor(wd_reference)

    return wd_reference

def search_wd_reference(
    search_label, 
    wd_reference, 
    wd_source=None, 
    return_var=None,
    source_priority_list=None, 
    preferred_only=False,
    return_original_label=False):

    items_from_label = [i for i in wd_reference if i["label"] == search_label]
    
    if wd_source is not None:
        items_from_label = [i for i in items_from_label if i["source"] == wd_source]

    if preferred_only:
        items_from_label = [i for i in items_from_label if i["label_source"] == "preferred"]

    if not items_from_label:
        if return_original_label:
            return search_label
        else:
            return

    if len(items_from_label) == 1:
        wd_item = items_from_label[0]
        if return_var is None:
            return items_from_label[0]
        else:
            return items_from_label[0][return_var]
    else:
        if source_priority_list is not None:
            preferred_wd_item = next((i for i in items_from_label if i["source"] in source_priority_list), None)
            if preferred_wd_item is not None:
                if return_var is None:
                    return preferred_wd_item
                else:
                    return preferred_wd_item[return_var]

        return items_from_label

def duplicates(lst, item):
    return [i for i, x in enumerate(lst) if x == item]

def wd_reference_uncertainty_factor(wd_reference_list):
    '''
    Takes a given Wikidata reference set, finds any duplicate labels, and introduces an uncertainty factor
    (number of label occurrences) and a "see also" reference of the other Wikidata entities in the given set
    for follow up.
    '''
    wd_reference_labels = [i["label"] for i in wd_reference_list]
    duplicate_labels = dict(
        (x, duplicates(wd_reference_labels, x)) for x in set(wd_reference_labels) if wd_reference_labels.count(x) > 1
    )

    for wd_ref in [i for i in wd_reference_list if i["label"] in duplicate_labels.keys()]:
        wd_ref.update(
            {
                "uncertainty_factor": len(duplicate_labels[wd_ref["label"]]),
                "uncertainty_see_also": [
                    i["identifier"] for index, i in enumerate(wd_reference_list) 
                    if index in duplicate_labels[wd_ref["label"]]
                    and i["identifier"] != wd_ref["identifier"]
                ]
            }
        )

    return wd_reference_list