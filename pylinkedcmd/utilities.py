import re

def actionable_id(identifier_string):
    identifiers = {
        "doi": {
            "pattern": r"10.\d{4,9}\/[\S]+$",
            "resolver": "https://doi.org/"
        },
        "orcid": {
            "pattern": r"\d{4}-\d{4}-\d{4}-\w{4}",
            "resolver": "https://orcid.org/"
        }
    }
    for k,v in identifiers.items():
        search = re.search(v["pattern"], identifier_string)
        if search:
            d_identifier = {
                k: search.group(),
                "url": f"{v['resolver']}{search.group().upper()}"
            }
            return d_identifier

    return None 
