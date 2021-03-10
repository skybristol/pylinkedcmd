import requests
from datetime import datetime
from . import utilities


def lookup_orcid(orcid, return_errors=False):
    '''
    This function handles the process of fetching a given ORCID using content negotiation to return the 
    JSON-LD structure from ORCID data. It checks for a number of error conditions and will either pass
    on those cases or return the errors for further consideration in a processing pipeline.
    '''
    identifiers = utilities.actionable_id(orcid)
    if identifiers is None:
        if return_errors:
            return {"orcid": orcid, "error": "Not a valid ORCID identifier"}
        else:
            return
    
    try:
        r = requests.get(identifiers["url"], headers={"accept": "application/ld+json"})
        if r.status_code != 200:
            if return_errors:
                return {"orcid": orcid, "error": f"HTTP Status Code: {str(r.status_code)}"}
            else:
                return
        else:
            raw_doc = r.json()
    except Exception as e:
        if return_errors:
            return {"orcid": orcid, "error": e}
        else:
            return

    if "givenName" not in raw_doc or "familyName" not in raw_doc:
        if return_errors:
            return {"orcid": orcid, "error": "Either givenName or familyName are missing from the ORCID record, and therefore it is unusable at this time."}
        else:
            return

    raw_doc["_date_cached"] = str(datetime.utcnow().isoformat())
    raw_doc["orcid"] = raw_doc["@id"].split("/")[-1]

    return raw_doc
