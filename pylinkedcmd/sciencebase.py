from sciencebasepy import SbSession
import requests
import validators
from datetime import datetime
import dateutil
from copy import copy
import unidecode
from getpass import getpass
import pandas as pd

class Directory:
    def __init__(self, authenticated=False):
        self.authenticated = authenticated
        self.sb_root_url = "https://www.sciencebase.gov/directory/people?format=json"
        self.sb_org_root = "https://www.sciencebase.gov/directory/organization/"
        self.sb_person_root = "https://www.sciencebase.gov/directory/person/"

        if authenticated:
            self.sb = SbSession()
            self.sb.login(input("User Name: "), getpass("Password: "))

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
            if self.authenticated:
                sb_results = self.sb._session.get(query_url).json()
            else:
                sb_results = requests.get(query_url).json()
        except:
            return None

        if len(sb_results["people"]) == 0 and attempt_last_name:
            name_criteria = criteria.split()[-1]
            query_url = f"{self.sb_root_url}&lastName={name_criteria}"
            try:
                if self.authenticated:
                    sb_results = self.sb._session.get(query_url).json()
                else:
                    sb_results = requests.get(query_url).json()
            except:
                return None
        elif len(sb_results["people"]) == 0 and not attempt_last_name:
            return None

        if unique and len(sb_results["people"]) == 1:
            return sb_results["people"][0]

        if not unique and verifier_operator is not None and verifier_criteria is not None:
            return next((i for i in sb_results["people"] if verifier_operator in i and i[verifier_operator] == verifier_criteria), None)

        if unique and len(sb_results["people"]) > 1:
            list_active = [i for i in sb_results["people"] if i["active"]]
            if len(list_active) == 1:
                return list_active[0]

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

        entity = {
            "entity_source": "ScienceBase Directory",
            "entity_created": datetime.utcnow().isoformat(),
            "reference": directory_item["link"]["href"],
            "instance_of": 'Person',
            "identifiers": identifiers,
            "name": directory_item["displayName"],
        }
        if "url" in directory_item and directory_item["url"] is not None:
            entity["url"] = [directory_item["url"]]
        if "description" in directory_item and directory_item["description"] is not None:
            entity["abstract"] = directory_item["description"]

        if self.authenticated:
            self.sb._session.headers.update({'Accept': 'text/html'})
            r = self.sb._session.get(identifiers["sbid"])
            tables = pd.read_html(r.text)
            entity["last_updated"] = dateutil.parser.parse(tables[0].loc[tables[0][0] == "Last Updated"][1].values[0])
            entity["last_updated_by"] = tables[0].loc[tables[0][0] == "Last Updated By"][1].values[0]

        statements_list = list()

        claim_constants = {
            "claim_created": datetime.utcnow().isoformat(),
            "claim_source": "ScienceBase Directory",
            "reference": identifiers["sbid"],
            "subject_instance_of": "Person",
            "subject_label": directory_item["displayName"],
            "subject_identifiers": identifiers
        }
        if "last_updated" in entity:
            claim_constants["date_qualifier"] = entity["last_updated"]
        else:
            claim_constants["date_qualifier"] = datetime.utcnow().isoformat()


        if "jobTitle" in directory_item:
            job_title_statement = copy(claim_constants)
            job_title_statement["property_label"] = "job title"
            job_title_statement["object_instance_of"] = "FieldOfWork"
            job_title_statement["object_label"] = directory_item["jobTitle"].title()
            job_title_statement["object_identifiers"] = None
            job_title_statement["object_qualifier"] = None
            statements_list.append(job_title_statement)
            
        if "organization" in directory_item:
            org_affiliation_statement = copy(claim_constants)
            org_affiliation_statement["property_label"] = "organization affiliation"
            org_affiliation_statement["object_instance_of"] = "Organization"
            org_affiliation_statement["object_label"] = directory_item["organization"]["displayText"].title()
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

                    inactive_employee_statement = copy(claim_constants)
                    inactive_employee_statement["property_label"] = "group affiliation"
                    inactive_employee_statement["object_instance_of"] = "Group"
                    inactive_employee_statement["object_label"] = "Inactive User Account"
                    statements_list.append(inactive_employee_statement)

            statements_list.append(org_affiliation_statement)

        result_doc = {
            "entity": entity,
            "claims": statements_list,
        }

        return result_doc