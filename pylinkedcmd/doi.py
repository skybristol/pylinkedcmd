import requests
from urllib.parse import urlparse
import re
from datetime import datetime
from jsonbender import bend, K, S, F, OptionalS
import json
from copy import deepcopy
from . import pylinkedcmd


class Lookup:
    def __init__(
        self, 
        doi, 
        source_doc=None, 
        output_format="summary", 
        include_source=False, 
        return_errors=False
    ):
        self.doi = doi
        self.source_doc = source_doc
        self.output_format = output_format
        self.include_source = include_source
        self.return_errors = return_errors
        self.headers = {"accept": "application/vnd.citationstyles.csl+json"}
        self.mapping = {
            'identifiers': F(
                lambda source:
                {'doi': source['DOI'], 'url': source['URL']} if "DOI" in source and "URL" in source else
                pylinkedcmd.actionable_id(self.doi)
            ),
            'entity_created': datetime.utcnow().isoformat(),
            'entity_source': 'DOI Metadata',
            'reference': pylinkedcmd.actionable_id(self.doi)["url"],
            'instance_of': F(
                lambda source:
                source['type'] if "type" in source else
                'CreativeWork'
            ),
            'is_part_of': F(
                lambda source:
                source["container-title"] if "container-title" in source else
                None
            ),
            'publisher': F(
                lambda source:
                source["publisher"] if "publisher" in source else
                None
            ),
            'name': S('title'),
            'date_published': F(
                lambda source:
                source["issued"]["date-parts"][0][0] if "DOI" in source else 
                None
            ),
            'abstract': F(
                lambda source:
                source["abstract"] if "abstract" in source else
                source["body"] if "body" in source else
                None
            )
        }

    def get_data(self, doi_url):
        if self.source_doc is not None:
            return self.source_doc
        else:
            try:
                r = requests.get(doi_url, headers=self.headers)
                if r.status_code != 200:
                    return {"doi": self.doi, "error": f"HTTP Status Code: {str(r.status_code)}"}
                else:
                    raw_doc = r.json()
            except Exception as e:
                return {"doi": self.doi, "error": e}

            return raw_doc

    def document(self):
        identifiers = pylinkedcmd.actionable_id(self.doi)

        if identifiers is None:
            if self.return_errors:
                return {"doi": self.doi, "error": "Not a valid DOI identifier"}
            else:
                return None

        raw_doc = self.get_data(doi_url=identifiers["url"])
        if "error" in raw_doc:
            if self.return_errors:
                return raw_doc
            else:
                return None

        if self.output_format == "summary":
            entity = bend(self.mapping, raw_doc)
            if self.include_source:
                entity["source"] = raw_doc

            if "DOI" in raw_doc:
                try:
                    r_citation = requests.get(
                        identifiers["url"], 
                        headers={"accept": "text/x-bibliography"}
                    )
                    if r_citation.status_code == 200:
                        entity["string_representation"] = r_citation.text
                except:
                    entity["string_representation"] = None

            return entity

        elif self.output_format == "cache":
            cache_document = {
                "identifiers": identifiers,
                "source": raw_doc
            }
            
            return cache_document

        else:
            return raw_doc

def character_balance(string, character=("(",")")):
    if string.count(character[0]) == string.count(character[1]):
        return True
    else:
        return False