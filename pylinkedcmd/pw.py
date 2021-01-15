import requests
import validators
from datetime import datetime
import dateutil
from copy import copy
import unidecode
import math
from . import pylinkedcmd
import hashlib

publication_api = "https://pubs.er.usgs.gov/pubs-services/publication"


class GetRecords:
    def __init__(self):
        self.publication_api = publication_api

    def pw_records(self, q=None, author_id=None, mod_x_days=None, page_size=1000):
        query_url = f"{self.publication_api}/?page_size={page_size}"
        if q is not None:
            query_url = f"{query_url}&q={q}"
        if author_id is not None:
            query_url = f"{query_url}&contributor={author_id}"
        if mod_x_days is not None:
            query_url = f"{query_url}&mod_x_days={mod_x_days}"

        r = requests.get(query_url)

        if r.status_code != 200:
            return {
                "query_url": query_url,
                "error": f"HTTP error: {r.headers}"
            }

        response_data = r.json()

        if "recordCount" not in response_data.keys():
            return {
                "query_url": query_url,
                "error": "No results in response"
            }

        records = response_data["records"]
        
        if response_data["recordCount"] > page_size:
            last_page_number = math.ceil(response_data["recordCount"] / page_size)
            for page_num in range(1, last_page_number):
                r = requests.get(f"{query_url}&page_number={page_num}")
                if r.status_code != 200:
                    break
                response_data = r.json()
                if response_data["records"]:
                    records.extend(response_data["records"])

        return records


class Summarize:
    def __init__(
        self, 
        pw_record, 
        only_usgs_contributors=True,
        try_doi_for_string_representation=True, 
        generate_uid=False
    ):
        self.description = "Set of functions for working with the USGS Pubs Warehouse REST API a little more better"
        self.publication_api = publication_api
        self.no_abstract_text_list = [
            "No abstract available.",
            "Abstract has not been submitted",
            "Abstract is unavailable.",
            "Abstract not available",
            "Abstract not available.",
            "Abstract not submitted to date",
            "Abstract not supplied at this time"
        ]
        self.property_class_mapping = {
            "authors": {
                "subject": "author of",
                "object": "authored by"
            },
            "editors": {
                "subject": "editor of",
                "object": "edited by"
            }
        }
        self.pw_record = pw_record
        self.only_usgs_contributors = only_usgs_contributors
        self.try_doi_for_string_representation = try_doi_for_string_representation
        self.generate_uid = generate_uid
        self.reference = f"{publication_api}/{pw_record['text'].split('-')[0].strip()}"
        self.source_name = "USGS Publications Warehouse"
        self.date_published = self.publication_date()

    def publication_date(self):
        if "publicationYear" in self.pw_record:
            return self.pw_record["publicationYear"]
        elif "displayToPublicDate" in self.pw_record:
            try:
                return datetime.strftime(dateutil.parser.parse(self.pw_record["displayToPublicDate"], '%Y'))
            except:
                return None

    def entity_stub(self):
        return {
            "reference": self.reference,
            "entity_created": datetime.utcnow().isoformat(),
            "entity_source": self.source_name,
        }

    def claim_stub(self):
        return {
            "reference": self.reference,
            "claim_created": datetime.utcnow().isoformat(),
            "claim_source": self.source_name,
            "date_qualifier": self.date_published
        }

    def generate_uid_from_identifiers(self, record):
        if "subject_identifiers" in record:
            source_identifiers = record["subject_identifiers"]
        else:
            source_identifiers = record["identifiers"]

        for id_type in ["doi","pw_url","orcid","email","pw_contributor_id"]:
            if id_type in source_identifiers:
                id_prop = id_type
                id_value = str(source_identifiers[id_type])
                break

        if "subject_identifiers" in record:
            id_value = ":".join([
                id_prop.upper(),
                id_value,
                record["property_label"],
                record["object_label"]
            ])

        return id_value, hashlib.md5(id_value.encode('utf-8')).hexdigest()

    def pub_entity(self):
        pub_entity = self.entity_stub()
        pub_entity["instance_of"] = self.pw_record["publicationType"]["text"]
        pub_entity["name"] = self.pw_record["title"]
        pub_entity["identifiers"] = dict()
        pub_entity["date_published"] = self.publication_date()

        try:
            pub_entity["publisher"] = self.pw_record["publisher"]
        except KeyError:
            pass

        try:
            pub_entity["identifiers"]["doi"] = self.pw_record["doi"]
            pub_entity["identifier_doi"] = self.pw_record["doi"]
        except KeyError:
            pass

        if not pub_entity["identifiers"]:
            pub_entity["identifiers"]["pw_url"] = self.reference

        if self.generate_uid:
            string_id, uid = self.generate_uid_from_identifiers(pub_entity)
            pub_entity["entity_id"] = uid

        try:
            pub_entity["is_part_of"] = \
                f'{self.pw_record["publicationSubtype"]["text"]}:{self.pw_record["seriesTitle"]["text"]}'
        except KeyError:
            try:
                pub_entity["is_part_of"] = self.pw_record["largerWorkTitle"]
            except KeyError:
                pass

        if "docAbstract" in self.pw_record and not any(x in self.pw_record["docAbstract"] for x in self.no_abstract_text_list):
            pub_entity["abstract"] = self.pw_record["docAbstract"]

        try:
            pub_entity["string_representation"] = self.pw_record["usgsCitation"]
        except KeyError:
            pass

        if "string_representation" not in pub_entity and "identifier_doi" in pub_entity and self.try_doi_for_string_representation:
            try:
                r_citation = requests.get(
                        f"https://doi.org/{pub_entity['identifier_doi']}", 
                        headers={"accept": "text/x-bibliography"}
                    )
                if r_citation.status_code == 200:
                    pub_entity["string_representation"] = r_citation.text
            except:
                pass

        return pub_entity

    def contributor_entity(self, contributor):
        contributor_entity = self.entity_stub()

        contributor_entity["instance_of"] = "Person"
        try:
            contributor_entity["name"] = f"{contributor['given']} {contributor['family']}"
        except KeyError:
            contributor_entity["name"] = contributor['text']
        contributor_entity["identifiers"] = dict()

        if "orcid" in contributor:
            identifiers = pylinkedcmd.actionable_id(contributor["orcid"])
            if identifiers is not None and "orcid" in identifiers:
                contributor_entity["identifiers"]["orcid"] = identifiers["orcid"]
                contributor_entity["identifier_orcid"] = identifiers["orcid"]

        if "email" in contributor and validators.email(contributor["email"].strip()):
            contributor_entity["identifiers"]["email"] = contributor["email"].strip()
            contributor_entity["identifier_email"] = contributor["email"].strip()

        if not contributor_entity["identifiers"]:
            contributor_entity["identifiers"]["pw_contributor_id"] = contributor["contributorId"]

        if self.generate_uid:
            string_id, uid = self.generate_uid_from_identifiers(contributor_entity)
            contributor_entity["entity_id"] = uid

        return contributor_entity

    def contributor_claims(self, pub_entity, contributor_entity, contributor_type):
        contributor_claim = self.claim_stub()
        contributor_claim["property_label"] = self.property_class_mapping[contributor_type]["subject"]
        contributor_claim["subject_instance_of"] = contributor_entity["instance_of"]
        contributor_claim["subject_label"] = contributor_entity["name"]
        contributor_claim["subject_identifiers"] = contributor_entity["identifiers"]
        for k, v in contributor_entity["identifiers"].items():
            contributor_claim[f"subject_identifier_{k}"] = v
        contributor_claim["object_instance_of"] = pub_entity["instance_of"]
        contributor_claim["object_label"] = pub_entity["name"]
        contributor_claim["object_identifiers"] = pub_entity["identifiers"]
        for k, v in pub_entity["identifiers"].items():
            contributor_claim[f"object_identifier_{k}"] = v
        if self.generate_uid:
            contributor_claim["claim_id"] = self.generate_uid_from_identifiers(contributor_claim)

        pub_claim = self.claim_stub()
        pub_claim["property_label"] = self.property_class_mapping[contributor_type]["object"]
        pub_claim["subject_instance_of"] = pub_entity["instance_of"]
        pub_claim["subject_label"] = pub_entity["name"]
        pub_claim["subject_identifiers"] = pub_entity["identifiers"]
        for k, v in pub_entity["identifiers"].items():
            pub_claim[f"subject_identifier_{k}"] = v
        pub_claim["object_instance_of"] = contributor_entity["instance_of"]
        pub_claim["object_label"] = contributor_entity["name"]
        pub_claim["object_identifiers"] = contributor_entity["identifiers"]
        for k, v in contributor_entity["identifiers"].items():
            pub_claim[f"object_identifier_{k}"] = v
        if self.generate_uid:
            string_id, uid = self.generate_uid_from_identifiers(pub_claim)
            pub_claim["claim_id"] = string_id
            pub_claim["claim_uid"] = uid

        return [contributor_claim, pub_claim]

    def digest(self):
        pub_entity = self.pub_entity()
        entities_and_claims = {
            "entities": [pub_entity],
            "claims": list()
        }
        if "contributors" in self.pw_record:
            for contributor_container, contributors in self.pw_record["contributors"].items():
                contributor_class = next((v for k,v in self.property_class_mapping.items() if k == contributor_container), None)
                if contributor_class is not None:
                    if self.only_usgs_contributors:
                        contributors_list = [i for i in contributors if i["usgs"]]
                    else:
                        contributors_list = contributors
                    for contributor in contributors_list:
                        contributor_entity = self.contributor_entity(contributor)
                        entities_and_claims["entities"].append(contributor_entity)
                        entities_and_claims["claims"].extend(
                            self.contributor_claims(
                                pub_entity, 
                                contributor_entity, 
                                contributor_type=contributor_container
                                )
                            )
        
        return entities_and_claims

