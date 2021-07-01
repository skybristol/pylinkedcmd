import re
import validators

def actionable_id(identifier_string, return_resolver=True):
    if validators.url(identifier_string) and "/staff-profiles/" in identifier_string.lower():
        return {
            "url": identifier_string,
            "profile": identifier_string.split("?")[0]
        }

    if validators.email(identifier_string):
        return {
            "email": identifier_string
        }

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
                k: search.group()
            }
            if return_resolver and v["resolver"] is not None:
                d_identifier["url"] = f"{v['resolver']}{search.group().upper()}"

            return d_identifier

    return 

def chunks(dict_list, chunk_size=1000):
    for i in range(0, len(dict_list), chunk_size):
        yield dict_list[i:i+chunk_size]

def doi_from_string(str_value):
    checker = re.findall(r'(10[.][0-9]{4,}[^\s"/<>]*/[^\s"<>]+)', str_value)
    link_part_pointers = [
        "/abstract",
        "/full",
        "/summary"
    ]
    
    for doi_string in checker:
        for part in link_part_pointers:
            if part in doi_string:
                doi_string.replace(part, '')
        if doi_string[-1] == ".":
            doi_string = doi_string[0:-1]
        
    return checker
