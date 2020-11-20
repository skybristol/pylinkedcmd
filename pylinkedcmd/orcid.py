import requests
import re
from urllib.parse import urlparse
from jsonbender import bend, K, S, F, OptionalS
import validators
from datetime import datetime
from copy import deepcopy
from . import pylinkedcmd


class Lookup:
    def __init__(self, orcid, orcid_doc=None, summarize=True, return_errors=False, include_source=False):
        self.orcid = orcid
        self.orcid_doc = orcid_doc
        self.summarize = summarize
        self.return_errors = return_errors
        self.include_source = include_source
        self.orcid_headers = {"accept": "application/ld+json"}
        self.mapping = {
            'identifiers': F(
                lambda source:
                pylinkedcmd.actionable_id(source['@id']) if "@id" in source else
                None
            ),
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

    def document(self):
        identifiers = pylinkedcmd.actionable_id(self.orcid)
        if identifiers is None:
            if self.return_errors:
                return {"orcid": self.orcid, "error": "Not a valid ORCID identifier"}
            else:
                return None
        
        if self.orcid_doc is None:
            try:
                r = requests.get(identifiers["url"], headers=self.orcid_headers)
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
                    return None
        else:
            raw_doc = self.orcid_doc

        if self.summarize:
            person_entity = bend(self.mapping, raw_doc)
            if self.include_source:
                person_entity["source"] = raw_doc
            claims = list()

            claim_constants = {
                "claim_created": datetime.utcnow().isoformat(),
                "claim_source": "ORCID",
                "reference": person_entity["identifiers"]["url"],
                "subject_instance_of": "Person",
                "subject_label": person_entity["name"],
                "subject_identifiers": {
                    "orcid": person_entity["identifiers"]["orcid"]
                }
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
                        item_claim["object_identifiers"] = item_data["identifiers"]
                        item_claim["object_instance_of"] = "CreativeWork"
                        item_claim["object_label"] = item_data["name"]
                        item_claim["property_label"] = "author of"
                        claims.append(item_claim)

                if "funder" in raw_doc["@reverse"]:
                    if isinstance(raw_doc["@reverse"]["funder"], dict):
                        funders = [raw_doc["@reverse"]["funder"]]
                    else:
                        funders = raw_doc["@reverse"]["funder"]
                    for item in funders:
                        item_data = bend(self.mapping, item)

                        item_claim = deepcopy(claim_constants)
                        item_claim["object_identifiers"] = item_data["identifiers"]
                        item_claim["object_instance_of"] = "Organization"
                        item_claim["object_label"] = item_data["name"]
                        item_claim["property_label"] = "funding organization"
                        claims.append(item_claim)

                        item_claim = deepcopy(claim_constants)
                        item_claim["object_identifiers"] = item_data["identifiers"]
                        item_claim["object_instance_of"] = "Project"
                        item_claim["object_label"] = item_data["alternateName"]
                        item_claim["property_label"] = "funded project"
                        claims.append(item_claim)

            if "affiliation" in raw_doc:
                if isinstance(raw_doc["affiliation"], dict):
                    affiliations = [raw_doc["affiliation"]]
                else:
                    affiliations = raw_doc["affiliation"]
                for item in affiliations:
                    item_data = bend(self.mapping, item)
                    item_claim = deepcopy(claim_constants)
                    item_claim["object_identifiers"] = item_data["identifiers"]
                    item_claim["object_instance_of"] = "Organization"
                    item_claim["object_label"] = item_data["name"]
                    item_claim["property_label"] = "organization affiliation"
                    claims.append(item_claim)

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
                    item_claim["object_identifiers"] = item_data["identifiers"]
                    item_claim["object_instance_of"] = "Organization"
                    item_claim["object_label"] = object_label
                    item_claim["property_label"] = "educational affiliation"
                    claims.append(item_claim)

            return {
                "entity": person_entity,
                "claims": claims
            }
        else:
            return raw_doc
