import requests
from datetime import datetime
from copy import copy
from . import utilities

def negotiate_doi(doi, response_type="registry", return_errors=False):
    identifiers = utilities.actionable_id(doi)

    if identifiers is None:
        if return_errors:
            return {"doi": doi, "error": "Not a valid DOI identifier"}
        else:
            return

    response_doc = {
        "_identifiers": identifiers,
        "_date": str(datetime.utcnow().isoformat())
    }

    if response_type == "registry":
        headers = {"accept": "application/vnd.citationstyles.csl+json"}
    elif response_type == "reference_string":
        headers = {"accept": "text/x-bibliography"}
    elif response_type == "dereference":
        headers = {"accept": "application/json"}

    try:
        r = requests.get(
            identifiers["url"], 
            headers=headers
        )
    except Exception as e:
        return {"doi": doi, "error": e}

    if r.status_code != 200:
        return {"doi": doi, "error": f"HTTP Status Code: {str(r.status_code)}"}
    else:
        if response_type == "reference_string":
            response_doc["reference_string"] = r.text
        else:
            try:
                response_doc.update(r.json())
            except:
                return {"doi": doi, "error": f"Content type with an accept header for JSON was not JSON"}

        return response_doc

def entity_from_doi(doi_doc):
    '''
    Processes a single DOI record retrieved via content negotiation into a flat summarized structure
    for processing into a graph or simple index. 
    '''
    if "error" in doi_doc:
        return

    summary_doc = {
        "doi": doi_doc["DOI"],
        "name": doi_doc["title"],
        "url": doi_doc["URL"],
        "publisher": doi_doc["publisher"],
        "date_qualifier": doi_doc["_date"]
    }

    if "issued" in doi_doc and isinstance(doi_doc["issued"]["date-parts"], list) and len(doi_doc["issued"]["date-parts"]) == 1:
        issued_year = doi_doc["issued"]["date-parts"][0][0]
        if issued_year is None:
            summary_doc["year_published"] = None
        else:
            summary_doc["year_published"] = str(issued_year)

    if doi_doc["type"] == "dataset":
        summary_doc["entity_type"] = "Dataset"
    else:
        summary_doc["entity_type"] = "CreativeWork"

    if "abstract" in doi_doc:
        summary_doc["description"] = doi_doc["abstract"]

    if "container-title" in doi_doc:
        if doi_doc["publisher"] == "US Geological Survey":
            summary_doc["journal"] = f"USGS {doi_doc['container-title']}"
        else:
            summary_doc["journal"] = doi_doc['container-title']

    if "event" in doi_doc:
        summary_doc["event"] = doi_doc["event"]

    return summary_doc

def doi_rel_stub(doi_doc):
    stub = {
        "doi": doi_doc["DOI"],
        "reference": doi_doc["URL"],
        "date_qualifier": None
    }
    if "issued" in doi_doc and isinstance(doi_doc["issued"]["date-parts"], list) and len(doi_doc["issued"]["date-parts"]) == 1:
        issued_year = doi_doc["issued"]["date-parts"][0][0]
        if issued_year is None:
            stub["date_qualifier"] = None
        else:
            stub["date_qualifier"] = int(issued_year)

    return stub

def funders_from_doi(doi_doc):
    if "funder" not in doi_doc:
        return list()

    funder_rels = list()
    for funder in doi_doc["funder"]:
        funder_rel = doi_rel_stub(doi_doc)
        funder_rel["name"] = funder["name"]
        funder_rel["rel_type"] = "FUNDER_OF"
        funder_rel["entity_type"] = "Organization"
        if "DOI" in funder:
            funder_rel["funder_doi"] = funder["DOI"]
        if funder["award"]:
            funder_rel["funder_award"] = ",".join(funder["award"])
        funder_rels.append(funder_rel)
    
    return funder_rels

def contacts_from_doi(doi_doc):
    if "author" not in doi_doc and "editor" not in doi_doc:
        return list()

    raw_contacts = list()
    if "author" in doi_doc:
        [i.update({"rel_type": "AUTHOR_OF"}) for i in doi_doc["author"]]
        raw_contacts.extend(doi_doc["author"])
    if "editor" in doi_doc:
        [i.update({"rel_type": "EDITOR_OF"}) for i in doi_doc["editor"]]
        raw_contacts.extend(doi_doc["editor"])

    contact_rels = list()
    for contact in [i for i in raw_contacts if "ORCID" in i]:
        contact_rel = doi_rel_stub(doi_doc)
        contact_rel["orcid"] = contact["ORCID"].split("/")[-1]
        contact_rel["sequence"] = contact["sequence"]
        contact_rel["rel_type"] = contact["rel_type"]
        contact_rel["entity_type"] = "Person"

        if "family" in contact and "given" not in contact:
            contact_rel["name"] = contact["family"]
        else:
            contact_rel["name"] = f"{contact['given']} {contact['family']}"
        contact_rels.append(contact_rel)

    return contact_rels

def terms_from_doi(doi_doc):
    if "categories" not in doi_doc and "subject" not in doi_doc:
        return list()

    raw_terms = list()
    if "categories" in doi_doc:
        raw_terms.extend(doi_doc["categories"])
    if "subject" in doi_doc:
        raw_terms.extend(doi_doc["subject"])

    term_rels = list()
    for term in raw_terms:
        term_rel = doi_rel_stub(doi_doc)
        term_rel["name"] = term
        term_rel["rel_type"] = "ADDRESSES_SUBJECT"
        term_rel["entity_type"] = "UndefinedSubjectMatter"
        term_rels.append(term_rel)

    return term_rels

