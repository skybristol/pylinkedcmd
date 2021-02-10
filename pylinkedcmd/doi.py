import requests
from datetime import datetime
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
        if r.status_code != 200:
            return {"doi": doi, "error": f"HTTP Status Code: {str(r.status_code)}"}
        else:
            if response_type == "reference_string":
                response_doc["reference_string"] = r.text
            else:
                response_doc.update(r.json)

            return response_doc

    except Exception as e:
        return {"doi": doi, "error": e}

