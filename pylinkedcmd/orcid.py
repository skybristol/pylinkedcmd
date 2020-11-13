import requests
import re
from urllib.parse import urlparse
from jsonbender import bend, K, S, F, OptionalS
import validators
from datetime import datetime
from copy import deepcopy
from collections import Counter
import json

class Lookup:
    def __init__(self, orcid, orcid_doc=None, summarize=True, return_errors=False):
        self.orcid = orcid
        self.orcid_doc = orcid_doc
        self.summarize = summarize
        self.return_errors = return_errors
        self.orcid_headers = {"accept": "application/ld+json"}
        self.mapping = {
            'identifiers': {'orcid': self.get_id()[0], 'url': self.get_id()[1]},
            'instance_of': S('@type'),
            'entity_created': datetime.utcnow().isoformat(),
            'entity_source': 'ORCID',
            'reference': OptionalS('@id'),
            'instance_of': 'Person',
            'name': F(
                lambda source:
                source["name"] if "name" in source else 
                f"{source['givenName']} {source['familyName']}"
                if "familyName" in source else 
                source["givenName"] if "givenName" in source else 
                None
            ),
            'alternateName': OptionalS('alternateName'),
            'url': F(
                lambda source:
                source["url"] if "url" in source
                and isinstance(source["url"], list) else
                [source["url"]] if "url" in source
                and not isinstance(source["url"], list) else
                None
            )
        }

    def get_id(self):
        if validators.url(self.orcid):
            orcid_string = urlparse(self.doi).path
            orcid_url = self.orcid
        else:
            orcid_string = self.orcid
            orcid_url = f"https://orcid.org/{self.orcid}"

        if re.match(r"\d{4}-\d{4}-\d{4}-\w{4}", orcid_string):
            return orcid_string, orcid_url
        else:
            return None, None

    def document(self):
        request_url=self.get_id()[1]
        if request_url is None:
            if self.return_errors:
                return {"orcid": self.orcid, "error": "Not a valid ORCID identifier"}
        else:
            if self.orcid_doc is None:
                try:
                    r = requests.get(request_url, headers=self.orcid_headers)
                    if r.status_code != 200:
                        if self.return_errors:
                            return {"orcid": self.orcid, "error": f"HTTP Status Code: {str(r.status_code)}"}
                        else:
                            return None
                    else:
                        raw_doc = r.json()
                except Exception as e:
                    if self.return_errors:
                        return {"orcid": self.orcid, "error": e}
            else:
                raw_doc = self.orcid_doc

        if self.summarize:
            person_entity = bend(self.mapping, raw_doc)
            person_entity["source"] = json.dumps(raw_doc)
            person_entity["claims"] = list()

            claim_constants = {
                "claim_created": datetime.utcnow().isoformat(),
                "claim_source": "ORCID",
                "reference": person_entity["identifiers"]["url"],
                "subject_instance_of": "Person",
                "subject_label": person_entity["name"],
                "subject_identifiers": json.dumps({
                    "orcid": person_entity["identifiers"]["orcid"]
                })
            }

            if "@reverse" in raw_doc:
                if "creator" in raw_doc["@reverse"]:
                    if isinstance(raw_doc["@reverse"]["creator"], dict):
                        creative_works = [raw_doc["@reverse"]["creator"]]
                    else:
                        creative_works = raw_doc["@reverse"]["creator"]
                    for item in creative_works:
                        item_claim = deepcopy(claim_constants)
                        item_data = bend(self.mapping, item)
                        item_claim["object_identifier"] = item_data["identifier"]
                        item_claim["object_instance_of"] = "CreativeWork"
                        item_claim["object_label"] = item_data["name"]
                        item_claim["property_label"] = "author of"
                        person_entity["claims"].append(item_claim)

                if "funder" in raw_doc["@reverse"]:
                    if isinstance(raw_doc["@reverse"]["funder"], dict):
                        funders = [raw_doc["@reverse"]["funder"]]
                    else:
                        funders = raw_doc["@reverse"]["funder"]
                    for item in funders:
                        item_data = bend(self.mapping, item)

                        item_claim = deepcopy(claim_constants)
                        item_claim["object_identifier"] = item_data["identifier"]
                        item_claim["object_instance_of"] = "Organization"
                        item_claim["object_label"] = item_data["name"]
                        item_claim["property_label"] = "funding organization"
                        person_entity["claims"].append(item_claim)

                        item_claim = deepcopy(claim_constants)
                        item_claim["object_identifier"] = item_data["identifier"]
                        item_claim["object_instance_of"] = "Project"
                        item_claim["object_label"] = item_data["alternateName"]
                        item_claim["property_label"] = "funded project"
                        person_entity["claims"].append(item_claim)

            if "affiliation" in raw_doc:
                if isinstance(raw_doc["affiliation"], dict):
                    affiliations = [raw_doc["affiliation"]]
                else:
                    affiliations = raw_doc["affiliation"]
                for item in affiliations:
                    item_data = bend(self.mapping, item)
                    item_claim = deepcopy(claim_constants)
                    item_claim["object_identifier"] = item_data["identifier"]
                    item_claim["object_instance_of"] = "Organization"
                    item_claim["object_label"] = item_data["name"]
                    item_claim["property_label"] = "organization affiliation"
                    person_entity["claims"].append(item_claim)

            if "alumniOf" in raw_doc:
                if isinstance(raw_doc["alumniOf"], dict):
                    affiliations = [raw_doc["alumniOf"]]
                else:
                    affiliations = raw_doc["alumniOf"]
                for item in affiliations:
                    item_data = bend(self.mapping, item)
                    if item_data["alternateName"] is not None:
                        object_label = f"{item_data['name']}, {item_data['alternateName']}"
                    else:
                        object_label = item_data["name"]

                    item_claim = deepcopy(claim_constants)
                    item_claim["object_identifier"] = item_data["identifier"]
                    item_claim["object_instance_of"] = "Organization"
                    item_claim["object_label"] = object_label
                    item_claim["property_label"] = "educational affiliation"
                    person_entity["claims"].append(item_claim)

            person_entity["entity"]["statements"] = json.dumps(dict(Counter([c["property_label"] for c in person_entity["claims"]])))

            return person_entity
        else:
            return raw_doc
