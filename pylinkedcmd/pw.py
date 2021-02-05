import requests
import validators
from datetime import datetime
import dateutil
from copy import copy
import unidecode
import math
import hashlib
from . import utilities

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
        try_doi_for_string_representation=True
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
                "object": "authored by",
                "instance_of": "Person",
                "bidirectional": True
            },
            "editors": {
                "subject": "editor of",
                "object": "edited by",
                "instance_of": "Person",
                "bidirectional": True
            },
            "publishers": {
                "subject": "publisher of",
                "object": "published by",
                "instance_of": "Organization",
                "bidirectional": True
            },
            "funders": {
                "subject": "funder of",
                "object": "funded by",
                "instance_of": "Organization",
                "bidirectional": True
            },
            "affiliations": {
                "subject": "affiliated with",
                "object": "affiliated with",
                "instance_of": "Organization",
                "bidirectional": True
            }
        }
        self.pw_record = pw_record
        self.only_usgs_contributors = only_usgs_contributors
        self.try_doi_for_string_representation = try_doi_for_string_representation

        self.reference = pw_public_url(pw_record)
        self.source_name = "USGS Publications Warehouse"
        self.pub_identifiers = pub_id(pw_record)
        self.label = pw_record["title"]
        self.pub_instance_of = "Publication"

        self.claim_stub = {
            "reference": self.reference,
            "claim_created": datetime.utcnow().isoformat(),
            "claim_source": self.source_name,
            "date_qualifier": publication_date(pw_record)
        }


    def pub_claims(self, claim_source, label_prop, property_class, id_prop=None):
        claims = list()

        subject_claim = copy(self.claim_stub)
        object_claim = copy(self.claim_stub)

        subject_claim["subject_label"] = claim_source[label_prop]
        object_claim["object_label"] = claim_source[label_prop]

        subject_claim["object_label"] = self.label
        object_claim["subject_label"] = self.label

        subject_claim["subject_instance_of"] = self.property_class_mapping[property_class]["instance_of"]
        subject_claim["object_instance_of"] = self.pub_instance_of
        object_claim["object_instance_of"] = self.property_class_mapping[property_class]["instance_of"]
        object_claim["subject_instance_of"] = self.pub_instance_of

        subject_claim["property_label"] = self.property_class_mapping[property_class]["subject"]
        object_claim["property_label"] = self.property_class_mapping[property_class]["object"]

        # Identifiers for pub
        subject_claim["object_identifiers"] = self.pub_identifiers["identifiers"]
        object_claim["subject_identifiers"] = self.pub_identifiers["identifiers"]
        for k,v in self.pub_identifiers.items():
            if k.split("_")[0] == "identifier":
                object_claim[f"subject_{k}"] = v
                subject_claim[f"object_{k}"] = v
        subject_claim["claim_id"] = claim_id(subject_claim)
        object_claim["claim_id"] = claim_id(object_claim)

        # Identifiers for claim subject/object
        if id_prop is not None:
            subject_claim["subject_identifiers"] = {
                id_prop: claim_source[id_prop]
            }
            if isinstance(claim_source[id_prop], dict):
                for k, v in claim_source[id_prop].items():
                    subject_claim[f"subject_identifier_{k}"] = v
                else:
                    object_claim[f"subject_identifier_{id_prop}"] = claim_source[id_prop]

            object_claim["object_identifiers"] = {
                id_prop: claim_source[id_prop]
            }
            if isinstance(claim_source[id_prop], dict):
                for k, v in claim_source[id_prop].items():
                    object_claim[f"object_identifier_{k}"] = v
                else:
                    object_claim[f"object_identifier_{id_prop}"] = claim_source[id_prop]

        claims.append(subject_claim)
        claims.append(object_claim)

        return claims

    def person_org_claims(self, claim_source):
        claims = list()

        person_claim = copy(self.claim_stub)
        org_claim = copy(self.claim_stub)

        person_claim["subject_label"] = claim_source["person"]["label"]
        person_claim["object_label"] = claim_source["org"]["label"]

        org_claim["subject_label"] = claim_source["org"]["label"]
        org_claim["object_label"] = claim_source["person"]["label"]

        person_claim["subject_instance_of"] = "Person"
        person_claim["object_instance_of"] = "Organization"

        org_claim["subject_instance_of"] = "Organization"
        org_claim["object_instance_of"] = "Person"

        person_claim["property_label"] = self.property_class_mapping[claim_source["property_class"]]["subject"]
        org_claim["property_label"] = self.property_class_mapping[claim_source["property_class"]]["object"]

        if claim_source["person"]["identifiers"] is not None:
            person_claim["subject_identifiers"] = claim_source["person"]["identifiers"]
            org_claim["object_identifiers"] = claim_source["person"]["identifiers"]

            for k, v in claim_source["person"]["identifiers"].items():
                person_claim[f"subject_identifier_{k}"] = v
                org_claim[f"object_identifier_{k}"] = v

        if claim_source["org"]["identifiers"] is not None:
            org_claim["subject_identifiers"] = claim_source["org"]["identifiers"]
            person_claim["object_identifiers"] = claim_source["org"]["identifiers"]

            for k, v in claim_source["org"]["identifiers"].items():
                org_claim[f"subject_identifier_{k}"] = v
                person_claim[f"object_identifier_{k}"] = v

        person_claim["claim_id"] = claim_id(person_claim)
        org_claim["claim_id"] = claim_id(org_claim)

        claims.append(person_claim)
        claims.append(org_claim)

        return claims

    def pub_link_claim(self, link_data, property_label="distributed by"):
        link_claim = copy(self.claim_stub)

        link_claim["subject_label"] = self.label
        link_claim["subject_instance_of"] = self.pub_instance_of

        # Identifiers for pub
        link_claim["subject_identifiers"] = self.pub_identifiers["identifiers"]
        for k,v in self.pub_identifiers["identifiers"].items():
            if k.split("_")[0] == "identifier":
                link_claim[f"subject_{k}"] = v

        link_claim["property_label"] = property_label
        link_claim["object_instance_of"] = "Web Link"

        link_claim["object_label"] = link_data["label"]
        link_claim["object_identifiers"] = {
            "url": link_data["url"]
        }
        link_claim["object_identifier_url"] = link_data["url"]

        link_claim["claim_id"] = claim_id(link_claim)

        return link_claim

    def extract_claims(self):
        claims = list()

        if "publisher" in self.pw_record:
            d_publisher = {
                "publisher": self.pw_record["publisher"]
            }
            claims.extend(self.pub_claims(d_publisher, "publisher", "publishers"))

        if "costCenters" in self.pw_record:
            for cost_center in self.pw_record["costCenters"]:
                d_cost_center = {
                    "pw_cost_center_id": cost_center["id"],
                    "label": cost_center["text"]
                }
                claims.extend(self.pub_claims(d_cost_center, "label", "funders", "pw_cost_center_id"))

        if "contributors" in self.pw_record:
            for contributor_type, contributor_list in self.pw_record["contributors"].items():
                for contributor in contributor_list:
                    d_contributor = dict()
                    if "given" in contributor and "family" in contributor:
                        d_contributor["label"] = f"{contributor['given']} {contributor['family']}"
                    else:
                        d_contributor["label"] = contributor["text"]

                    if "email" in contributor or "orcid" in contributor:
                        d_contributor["identifiers"] = dict()
                        if "email" in contributor:
                            d_contributor["identifiers"]["email"] = contributor["email"]
                        if "orcid" in contributor:
                            d_contributor["identifiers"]["orcid"] = contributor["orcid"].split("/")[-1]
                        
                        id_prop = "identifiers"
                    else:
                        id_prop = None

                    if "affiliations" in contributor and len(contributor["affiliations"]) > 0:
                        for affiliation in contributor["affiliations"]:
                            d_affiliation = {
                                "property_class": "affiliations",
                                "org": {
                                    "label": affiliation["text"],
                                    "identifiers": None
                                },
                                "person": {
                                    "label": d_contributor["label"],
                                    "identifiers": None
                                }
                            }
                            if id_prop is not None:
                                d_affiliation["person"]["identifiers"] = d_contributor["identifiers"]

                            claims.extend(self.person_org_claims(d_affiliation))

                    claims.extend(self.pub_claims(d_contributor, "label", contributor_type, id_prop))

        if "links" in self.pw_record:
            for link in [l for l in self.pw_record["links"] if "description" in l or "text" in l]:
                d_link = {
                    "url": link["url"]
                }
                if "description" in link:
                    d_link["label"] = f'{link["description"]} ({link["url"]})'
                else:
                    d_link["label"] = f'{link["text"]} ({link["url"]})'
                claims.append(self.pub_link_claim(d_link))

        return claims


def pw_public_url(pw_record):
    return f"{publication_api}/{pw_record['text'].split('-')[0].strip()}"

def pub_id(pw_record):
    identifiers = {
        "identifiers": {
            "pw_pub_id": pw_record["id"],
            "pw_url": pw_public_url(pw_record)
        }
    }
    if "doi" in pw_record:
        identifiers["identifiers"]["doi"] = pw_record["doi"]
        use_for_uid = pw_record["doi"]
    else:
        use_for_uid = identifiers["identifiers"]["pw_url"]
    
    identifiers["pub_uid"] = hashlib.md5(use_for_uid.encode('utf-8')).hexdigest()

    for k, v in identifiers["identifiers"].items():
        identifiers[f"identifier_{k}"] = v

    return identifiers

def publication_date(pw_record):
    if "publicationYear" in pw_record:
        return pw_record["publicationYear"]
    elif "displayToPublicDate" in pw_record:
        try:
            return datetime.strftime(dateutil.parser.parse(pw_record["displayToPublicDate"], '%Y'))
        except:
            return None

def claim_id(claim):
    id_string = ":".join([
        claim["subject_label"],
        claim["property_label"],
        claim["object_label"]
    ])

    return id_string
