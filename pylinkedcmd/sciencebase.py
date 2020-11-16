from sciencebasepy import SbSession
import requests
import validators
from datetime import datetime
from copy import copy
import unidecode

class Directory:
    def __init__(self):
        self.sb_root_url = "https://www.sciencebase.gov/directory/people?format=json"
        self.sb_org_root = "https://www.sciencebase.gov/directory/organization/"
        self.sb_person_root = "https://www.sciencebase.gov/directory/person/"

    def lookup_person(
        self, 
        criteria, 
        unique=True, 
        verifier_operator=None, 
        verifier_criteria=None, 
        attempt_last_name=True
    ):
        q_operator = "q"
        if validators.email(criteria):
            q_operator = "email"
        else:
            criteria = f"{criteria.split()[0]} {criteria.split()[-1]}"
            criteria = unidecode.unidecode(criteria)

        if verifier_operator is not None:
            unique = False
        
        query_url = f"{self.sb_root_url}&{q_operator}={criteria}"

        try:
            sb_results = requests.get(query_url).json()
        except:
            return None

        if len(sb_results["people"]) == 0 and attempt_last_name:
            name_criteria = criteria.split()[-1]
            query_url = f"{self.sb_root_url}&lastName={name_criteria}"
            try:
                sb_results = requests.get(query_url).json()
            except:
                return None
        elif len(sb_results["people"]) == 0 and not attempt_last_name:
            return None

        if unique and len(sb_results["people"]) == 1:
            return sb_results["people"][0]

        if not unique and verifier_operator is not None and verifier_criteria is not None:
            return next((i for i in sb_results["people"] if verifier_operator in i and i[verifier_operator] == verifier_criteria), None)

        if not unique and len(sb_results["people"]) > 1:
            return sb_results["people"]

        return None

    def summarize_entity(
        self, 
        criteria=None, 
        directory_item=None, 
        build_entity=False,
        unique=True, 
        verifier_operator=None, 
        verifier_criteria=None, 
        attempt_last_name=True
    ):
        if directory_item is None and criteria is not None:
            directory_item = self.lookup_person(
                criteria,
                unique,
                verifier_operator,
                verifier_criteria,
                attempt_last_name
            )

        if directory_item is None or not isinstance(directory_item, dict):
            return None
        
        identifiers = {
            "sbid": directory_item["link"]["href"],
        }

        if "email" in directory_item:
            identifiers["email"] = directory_item["email"]

        if "orcId" in directory_item:
            identifiers["orcid"] = directory_item["orcId"]

        statements_list = list()

        claim_constants = {
            "claim_created": datetime.utcnow().isoformat(),
            "claim_source": "ScienceBase Directory",
            "reference": identifiers["sbid"],
            "subject_instance_of": "Person",
            "subject_label": directory_item["displayName"],
            "subject_identifiers": identifiers
        }

        if "jobTitle" in directory_item:
            job_title_statement = copy(claim_constants)
            job_title_statement["property_label"] = "job title"
            job_title_statement["object_instance_of"] = "FieldOfWork"
            job_title_statement["object_label"] = directory_item["jobTitle"].title()
            job_title_statement["date_qualifier"] = datetime.utcnow().isoformat()
            job_title_statement["object_identifiers"] = None
            job_title_statement["object_qualifier"] = None
            statements_list.append(job_title_statement)
            
        if "organization" in directory_item:
            org_affiliation_statement = copy(claim_constants)
            org_affiliation_statement["property_label"] = "organization affiliation"
            org_affiliation_statement["object_instance_of"] = "Organization"
            org_affiliation_statement["object_label"] = directory_item["organization"]["displayText"].title()
            org_affiliation_statement["date_qualifier"] = datetime.utcnow().isoformat()
            org_affiliation_statement["object_identifiers"] = {
                "sbid": f"{self.sb_org_root}{directory_item['organization']['id']}"
            }
            try:
                org_affiliation_statement["object_identifiers"]["fbms_code"] = directory_item["extensions"]["usgsPersonExtension"]["orgCode"]
            except:
                pass
            if "active" in directory_item:
                if directory_item["active"]:
                    org_affiliation_statement["object_qualifier"] = "Current active employee as of date_qualifier"
                else:
                    org_affiliation_statement["object_qualifier"] = "No longer an active employee"
            statements_list.append(org_affiliation_statement)

        result_doc = {
            "identifiers": identifiers,
            "claims": statements_list,
        }

        if build_entity:
            result_doc["entity_created"] = datetime.utcnow().isoformat()
            result_doc["entity_source"] = "ScienceBase Directory"
            result_doc["name"] = directory_item["displayName"]
            result_doc["reference"] = identifiers["sbid"]
            result_doc["instance_of"] = "Person"
            if "url" in directory_item:
                result_doc["url"] = [directory_item["url"]]
            if "description" in directory_item:
                result_doc["abstract"] = directory_item["description"]

        return result_doc