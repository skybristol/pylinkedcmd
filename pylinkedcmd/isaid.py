import requests
from datetime import datetime
from . import utilities
import dateutil.parser
import validators

def model_node_from_sb_item(item):
    relationship_mapping = [
        {
            "sb_labels": ["sourceCode", "Source Code"],
            "node_type": "SourceCodeRepository",
            "relationship": "SOURCED_FROM",
            "container": "source_repositories"
        },
        {
            "sb_labels": ["Citation", "Publication", "Publication "],
            "node_type": "CreativeWork",
            "relationship": "REFERENCE",
            "container": "references"
        },
        {
            "sb_labels": ["Model Output"],
            "node_type": "Dataset",
            "relationship": "MODEL_OUTPUT",
            "container": "datasets"
        },
        {
            "sb_labels": ["Contact"],
            "container": "points_of_contact",
            "relationship": "POINT_OF_CONTACT"
        },
        {
            "sb_labels": ["Author", "Creator"],
            "container": "authors",
            "relationship": "AUTHOR_OF"
        },
        {
            "sb_labels": ["Cooperator/Partner"],
            "container": "contributors",
            "relationship": "CONTRIBUTOR"
        },
        {
            "sb_labels": ["Lead Organization", "Organization"],
            "container": "organizations",
            "relationship": "AFFILIATED_WITH"
        },
        {
            "sb_labels": ["USGS Mission Area"],
            "container": "organizations",
            "node_type": "Organization",
            "relationship": "AFFILIATED_WITH",
            "ignore_names": ["External Model"],
            "add_to_name": " Mission Area"
        },
    ]

    model = {
        "properties": {
            "node_type": "ScientificModel",
            "title": item["title"],
            "alternateName": "",
            "url": item["link"]["url"],
            "description": item["summary"],
            "date_created": item["provenance"]["dateCreated"],
            "last_updated": item["provenance"]["lastUpdated"],
            "image_url": "",
            "image_title": ""
        },
        "relationships": dict()
    }

    date_qualifier = item["provenance"]["lastUpdated"]

    title_parts = item["title"].split(" - ")
    if len(title_parts) > 1:
        model["properties"]["name"] = title_parts[0].strip()
    else:
        model["properties"]["name"] = item["title"]

    if "subTitle" in item:
        model["properties"]["alternateName"] = item["subTitle"]

    if "previewImage" in item and "original" in item["previewImage"]:
        model["properties"]["image_url"] = item["previewImage"]["original"]["viewUri"]
        if "title" in item["previewImage"]["original"]:
            model["properties"]["image_title"] = item["previewImage"]["original"]["title"]

    if "contacts" in item:
        for contact in [c for c in item["contacts"] if "type" in c and "contactType" in c]:
            mapping = next((i for i in relationship_mapping if contact["type"] in i["sb_labels"]), None)
            if mapping is not None:
                if "ignore_names" in mapping and contact["name"] in mapping["ignore_names"]:
                    continue

                if "add_to_name" in mapping:
                    node_name = f"{contact['name']}{mapping['add_to_name']}"
                else:
                    node_name = contact["name"]

                if "node_type" in mapping:
                    node_type = mapping["node_type"]
                else:
                    node_type = contact["contactType"].title()

                contact_node = {
                    "node_type": node_type,
                    "name": node_name,
                    "reference": item["link"]["url"],
                    "date_qualifier": date_qualifier,
                    "relationship_type": mapping["relationship"]
                }

                relationship_container = mapping["container"]
                if "email" in contact:
                    contact_node["email"] = contact["email"]
                    relationship_container  = f"identified_{mapping['container']}"
                if "orcId" in contact:
                    contact_node["orcid"] = contact["orcId"]
                    relationship_container  = f"identified_{mapping['container']}"
                if "oldPartyId" in contact:
                    contact_node["identifier_sciencebase"] = f'https://www.sciencebase.gov/directory/person/{contact["oldPartyId"]}'
                    relationship_container  = f"identified_{mapping['container']}"

                if relationship_container not in model["relationships"]:
                    model["relationships"][relationship_container] = list()
                
                model["relationships"][relationship_container].append(contact_node)

    if "webLinks" in item:
        for link in [i for i in item["webLinks"] if "title" in i]:
            if "typeLabel" not in link:
                continue

            mapping = next((i for i in relationship_mapping if link["typeLabel"] in i["sb_labels"]), None)
            if mapping is not None:
                link_node = {
                    "node_type": mapping["node_type"],
                    "relationship_type": mapping["relationship"],
                    "url": link["uri"],
                    "date_qualifier": date_qualifier,
                    "reference": item["link"]["url"]
                }
                if link["title"] == "Code Repository":
                    link_node["name"] = f'{model["properties"]["name"]} Code Repository'
                else:
                    link_node["name"] = link["title"]

                relationship_container = mapping["container"]
                id_from_link = utilities.actionable_id(link["uri"])
                if id_from_link is not None and "doi" in id_from_link:
                    link_node["doi"] = id_from_link["doi"]
                    relationship_container  = f"identified_{mapping['container']}"

                if relationship_container not in model["relationships"]:
                    model["relationships"][relationship_container] = list()
                
                model["relationships"][relationship_container].append(link_node)

    return model


def package_source_scientific_models():
    api_endpoint = "https://www.sciencebase.gov/catalog/items?&max=200&folderId=5ed7d36182ce7e579c66e3be&format=json&fields=title,subTitle,summary,contacts,tags,webLinks,provenance,previewImage"
    sb_response = requests.get(api_endpoint).json()

    model_items = [
        model_node_from_sb_item(item) for item
        in sb_response["items"]
    ]

    return model_items


def dataset_node_from_sdc_item(item):
    contact_type_mapping = {
        "Organizational": "Organization",
        "Personal": "Person",
        "USGSPersonal": "Person"
    }

    dataset = {
        "properties": {
            "node_type": "Dataset",
            "name": item["title"],
            "url": f"https://data.usgs.gov/datacatalog/data/{item['identifier']}",
            "description": item["description"].strip(),
            "issued_year": ""
        },
        "relationships": {
            "identified_points_of_contact": list(),
            "unidentified_points_of_contact": list(),
            "identified_authors": list(),
            "unidentified_authors": list(),
            "organizations": list(),
            "places": list(),
            "defined_terms": list(),
            "undefined_terms": list()
        }
    }

    if "modified" in item:
        try:
            date_qualifier = str(dateutil.parser.parse(item["modified"]).isoformat())
        except:
            date_qualifier = str(datetime.utcnow().isoformat())
    elif "@timestamp" in item:
        date_qualifier = item["@timestamp"]
    else:
        date_qualifier = str(datetime.utcnow().isoformat())

    dataset["properties"]["last_updated"] = date_qualifier

    if "issued" in item:
        dataset["properties"]["issued_year"] = item["issued"]

    if "contactPoint" in item and "hasEmail" in item["contactPoint"] and item["contactPoint"]["hasEmail"] is not None:
        poc_node = {
            "node_type": "Person",
            "node_relationship": "POINT_OF_CONTACT",
            "name": item["contactPoint"]["fn"],
            "date_qualifier": date_qualifier,
            "reference": dataset["properties"]["url"],
        }
        email_string = item["contactPoint"]["hasEmail"].split(" ")[-1].strip().lower()
        if validators.email(email_string):
            poc_node["email"] = email_string
            dataset["relationships"]["identified_points_of_contact"].append(poc_node)
        else:
            dataset["relationships"]["unidentified_points_of_contact"].append(poc_node)
    
    if "authors" in item:
        for author in [a for a in item["authors"] if isinstance(a, dict) and "nametype" in a]:
            author_node = {
                "name": author["authorname"],
                "node_type": contact_type_mapping[author["nametype"]],
                "node_relationship": "AUTHOR_OF",
                "date_qualifier": date_qualifier,
                "reference": dataset["properties"]["url"]
            }
            if author["orcid"]:
                author_node["orcid"] = author["orcid"]
                dataset["relationships"]["identified_authors"].append(author_node)
            else:
                dataset["relationships"]["unidentified_authors"].append(author_node)

    if "datasource" in item:
        for org_name in [i["displayname"] for i in item["datasource"] if "displayname" in i]:
            dataset["relationships"]["organizations"].append({
                "name": org_name,
                "node_type": "Organization",
                "node_relationship": "DATA_SOURCE",
                "date_qualifier": date_qualifier,
                "reference": dataset["properties"]["url"]
            })

    if "placeKeyword" in item:
        for place in item["placeKeyword"]:
            dataset["relationships"]["places"].append({
                "name": place,
                "node_type": "Place",
                "node_relationship": "ADDRESSES_SUBJECT",
                "date_qualifier": date_qualifier,
                "reference": dataset["properties"]["url"]
            })

    if "usgsThesaurusKeyword" in item:
        for term in item["usgsThesaurusKeyword"]:
            dataset["relationships"]["defined_terms"].append({
                "name": term,
                "node_type": "DefinedSubjectMatter",
                "category": "USGS Thesaurus",
                "node_relationship": "ADDRESSES_SUBJECT",
                "date_qualifier": date_qualifier,
                "reference": dataset["properties"]["url"]
            })

    if "isoTopicKeyword" in item:
        if not isinstance(item["isoTopicKeyword"], list):
            item["isoTopicKeyword"] = [item["isoTopicKeyword"]]
        for term in item["isoTopicKeyword"]:
            dataset["relationships"]["defined_terms"].append({
                "name": term,
                "node_type": "DefinedSubjectMatter",
                "category": "ISO Topic Keyword for Geospatial Metadata",
                "node_relationship": "ADDRESSES_SUBJECT",
                "date_qualifier": date_qualifier,
                "reference": dataset["properties"]["url"]
            })

    if "otherKeyword" in item:
        for term in item["otherKeyword"]:
            dataset["relationships"]["undefined_terms"].append({
                "name": term,
                "node_type": "UndefinedSubjectMatter",
                "category": "dataset descriptive keywords",
                "node_relationship": "ADDRESSES_SUBJECT",
                "date_qualifier": date_qualifier,
                "reference": dataset["properties"]["url"]
            })

    return dataset

def work_node_from_doi_doc(doi_doc):
    if "title" not in doi_doc or doi_doc["title"] is None or len(doi_doc["title"]) == 0:
        return

    doi_type_mapping = {
        'other': 'Document',
        'monograph': 'Document',
        'article-journal': 'Document',
        'posted-content': 'Document',
        'article': 'Document',
        'book-section': 'Document',
        'proceedings': 'Document',
        'graphic': 'Document',
        'paper-conference': 'Document',
        'reference-entry': 'Document',
        'chapter': 'Document',
        'dataset': 'Dataset',
        'report': 'Document',
        'thesis': 'Document',
        'peer-review': 'Document',
        'reference-book': 'Document',
        'journal-issue': 'Document',
        'book-part': 'Document',
        'component': 'Document',
        'standard': 'Document',
        'book': 'Document'
    }
            
    doi_node = {
        "properties": {
            "name": doi_doc["title"],
            "url": doi_doc["URL"],
            "doi": doi_doc["DOI"],
            "issued_year": doi_doc["issued"]["date-parts"][0][0]
        },
        "linkages": list()
    }

    date_qualifier = doi_node["issued_year"]

    if "type" not in doi_doc or doi_doc["type"] is None or doi_doc["type"] not in list(doi_type_mapping.keys()):
        doi_node["properties"]["node_type"] = "Document"
    else:
        doi_node["properties"]["node_type"] = doi_type_mapping[doi_doc["type"]]

    if "reference_string" in doi_doc:
        doi_node["properties"]["reference_string"] = doi_doc["reference_string"]

    if "subject" in doi_doc:
        for subject in doi_doc["subject"]:
            doi_node["linkages"].append({
                "node_type": "SubjectMatter",
                "relationship_type": "ADDRESSES_SUBJECT",
                "node_name": subject,
                "date_qualifier": date_qualifier
            })

    if "publisher" in doi_doc:
        doi_node["linkages"].append({
            "node_type": "Organization",
            "relationship_type": "PUBLISHED_BY",
            "node_name": doi_doc["publisher"],
            "category": "Publishing Institution",
            "date_qualifier": date_qualifier
        })

    if "container-title" in doi_doc:
        if "publisher" in doi_doc and doi_doc["publisher"] == "US Geological Survey":
            journal_name = f"USGS Series Report: {doi_doc['container-title']}"
        else:
            journal_name = doi_doc['container-title']
        doi_node["linkages"].append({
            "node_type": "Journal",
            "relationship_type": "PUBLISHED_IN",
            "node_name": journal_name,
            "date_qualifier": date_qualifier
        })

    if "funder" in doi_doc:
        for funder in doi_doc["funder"]:
            funder_node = {
                "node_type": "Organization",
                "relationship_type": "FUNDER_OF",
                "node_name": funder["name"],
                "category": "Funding Institution",
                "date_qualifier": date_qualifier
            }
            if "DOI" in funder:
                funder_node["identifier_doi"] = funder["DOI"]

            if len(funder["award"]) > 0:
                funder_node["award"] = funder["award"]
            
            doi_node["linkages"].append(funder_node)

    if "author" in doi_doc:
        for author in doi_doc["author"]:
            author_node = {
                "properties": {
                    "node_type": "Person",
                    "relationship_type": "AUTHOR_OF",
                    "date_qualifier": date_qualifier
                }
            }
            if "name" in author:
                author_node["properties"]["name"] = author["name"]
            elif "literal" in author:
                author_node["properties"]["name"] = author["literal"]
            elif "family" in author and "given" in author:
                author_node["properties"]["name"] = f"{author['given']} {author['family']}"
            elif "family" in author:
                author_node["properties"]["name"] = author["family"]
            else:
                continue

            if "ORCID" in author:
                author_node["properties"]["url"] = author["ORCID"]
                author_node["properties"]["orcid"] = author["ORCID"].split("/")[-1]

            if "sequence" in author:
                author_node["properties"]["relationship_qualifier"] = author["sequence"]

            if "affiliation" in author and len(author["affiliation"]) > 0:
                author_node["linkages"] = list()
                for affiliation in author["affiliation"]:
                    author_node["linkages"].append({
                        "name": affiliation,
                        "node_type": "Organization",
                        "relationship_type": "AFFILIATED_WITH",
                        "date_qualifier": date_qualifier
                    })
            
            doi_node["linkages"].append(author_node)

    return doi_node