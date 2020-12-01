import requests
from datetime import datetime
import dateutil
from sciencebasepy import SbSession
import validators
from fuzzywuzzy import fuzz
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import json
from getpass import getpass
import pydash
import copy
import re
from bs4 import BeautifulSoup
import pandas as pd
import math
from nltk.tokenize import sent_tokenize
from copy import deepcopy
from jsonbender import bend, K, S, F, OptionalS


class Sciencebase:
    def __init__(self):
        self.description = "Set of functions for working with the ScienceBase Directory"
        self.sb_directory_people_api = "https://www.sciencebase.gov/directory/people"
        self.sb_directory_org_api = "https://www.sciencebase.gov/directory/organization"
        self.fix_identifiers = ["ORCID", "WikiData"]
        self.wd = Wikidata()
        self.sb_directory_base_url = "https://www.sciencebase.gov/directory/"
        self.ignore_org_names = [
            "ASK USGS -- Water Webserver Team",
            "U.S. Geological Survey - ScienceBase"
        ]
        self.cmd_isaid = Isaid()
        self.org_mapping = {
            'identifier': S('links', 0, 'url'),
            'name': S('name'),
            'url': OptionalS('url'),
            'alternateName': OptionalS('aliases', 0, 'name'),
            'addressLocality': OptionalS('primaryLocation', 'streetAddress', 'city'),
            'addressRegion': OptionalS('primaryLocation', 'streetAddress', 'state'),
            'region': OptionalS('extensions', 'usgsOrganization', 'region'),
            'usgsMissionAreas': F(
                lambda source: 
                [k["displayText"] for k 
                    in source["extensions"]["usgsOrganization"]["usgsMissionAreas"]
                ] if exists(source, ["extensions","usgsOrganization","usgsMissionAreas"])
                else None
            ),
            'usgsPrograms': F(
                lambda source: 
                [k["displayText"] for k 
                    in source["extensions"]["usgsOrganization"]["usgsPrograms"]
                ] if exists(source, ["extensions","usgsOrganization","usgsPrograms"])
                else None
            )
        }

    def summarize_sb_person(self, person_doc):
        ignore_props = [
            "_classSimpleName",
            "distinguishedName",
            "name",
            "type",
            "displayText",
            "richDescriptionHtml",
            "username",
            "organizationDisplayText"
        ]

        new_person_doc = {
            "identifier_sbid": person_doc["link"]["href"],
            "date_cached": datetime.utcnow().isoformat()
        }
        for k, v in person_doc.items():
            if k not in ignore_props and isinstance(v, str):
                new_person_doc[k.lower()] = v

        if "identifiers" in person_doc.keys():
            for i in person_doc["identifiers"]:
                new_person_doc[f"identifier_{i['type'].lower()}"] = i["key"]

        if "orcid" in new_person_doc.keys() \
            and len(new_person_doc["orcid"]) > 0 \
            and ( \
                "identifier_orcid" not in new_person_doc.keys() \
                or new_person_doc["identifier_orcid"] is None \
                or len(new_person_doc["identifier_orcid"]) < 19):
            new_person_doc["identifier_orcid"] = new_person_doc["orcid"]

        try:
            new_person_doc["organization_name"] = person_doc["organization"]["displayText"]
        except KeyError:
            pass

        try:
            new_person_doc[
                "organization_uri"] = f'{self.sb_directory_org_api}/{person_doc["organization"]["id"]}'
        except KeyError:
            pass

        try:
            new_person_doc["city"] = person_doc["primaryLocation"]["streetAddress"]["city"]
        except KeyError:
            pass

        try:
            new_person_doc["state"] = person_doc["primaryLocation"]["streetAddress"]["state"]
        except KeyError:
            pass

        if "email" in new_person_doc.keys():
            new_person_doc["email"] = new_person_doc["email"].lower()
            new_person_doc["identifier_email"] = new_person_doc["email"].lower()

        return new_person_doc

    def get_active_usgs_staff(self, return_format="summarized"):
        sb_dir_next_link = "https://www.sciencebase.gov/directory/people?format=json&dataset=all&active=True&max=1000"
        sb_dir_results = list()

        while sb_dir_next_link is not None:
            r = requests.get(sb_dir_next_link).json()
            if len(r["people"]) > 0:
                sb_dir_results.extend(r["people"])
            else:
                break
            if "nextlink" in r.keys():
                sb_dir_next_link = r["nextlink"]["url"]
            else:
                break

        if return_format == "summarized":
            return [self.summarize_sb_person(i) for i in sb_dir_results]

        return sb_dir_results

    def get_staff_by_email(self, email_list, return_format="summarized", use_auth_search=False):
        if use_auth_search:
            try:
                sb = SbSession()
                sb.login(input("User Name: "), getpass("Password: "))
            except Exception as e:
                raise ValueError(f"Something went wrong in trying to authenticate to ScienceBase: {e}")

        sb_dir_results = list()

        for email in email_list:
            if validators.email(email):
                query = f"https://www.sciencebase.gov/directory/people?format=json&email={email}"
                if use_auth_search:
                    r = sb._session.get(query).json()
                else:
                    r = requests.get(query).json()

                sb_dir_results.extend(r["people"])

        if return_format == "summarized":
            return [self.summarize_sb_person(i) for i in sb_dir_results]

        return sb_dir_results

    def lookup_sb_person_by_email(self, email):
        '''
        Searches the ScienceBase Directory for a person by their email address. Returns None if either no record or
        more than one record is found because we cannot act on these in either case.

        :param email: Must be a valid email string
        :return: Either None if not found or the correctly formatted full person document (dict)
        '''
        #if not validators.email(email):
        #    raise ValueError(f"You must supply a valid email address: {email}")

        r = requests.get(
            f"{self.sb_directory_people_api}?format=json&email={email}"
        ).json()

        if len(r["people"]) != 1:
            if email.split("@")[-1].lower() == "usgs.gov":
                alternate_email = f'{email.split("@")[0].lower()}@contractor.usgs.gov'

                r = requests.get(
                    f"{self.sb_directory_people_api}?format=json&email={alternate_email}"
                ).json()

        if len(r["people"]) != 1:
            return None

        # We have to get the person record with a separate process because it's different than what comes
        # back in the search result
        person_record = requests.get(f'{r["people"][0]["link"]["href"]}?format=json').json()

        return person_record

    def identified_sb_person(self, person):
        '''
        Function does some specialized stuff with ScienceBase Directory person documents to package identifiers into
        the identifiers object in a way that supports linked data operations. The purpose here is to generate a
        revised person document to commit to the ScienceBase Directory that supports further operations, based on the
        available identifiers, such as dynamically assembling a research record (publications, datasets, etc.).

        :param person: Dict containing a minimum of an email property that will kick off a series of lookups; may
        contain already established API url to ScienceBase Directory identity, orcid, and/or wikidata_id
        :return: Python set containing a revised person record with new identifiers object and a boolean value indicating
        whether or not the given person record was modified with the new identifiers construct
        '''
        person_record = None

        if "api" in person.keys():
            if not validators.url(person["api"]):
                raise ValueError("The API supplied must be in a valid URL format")

            person_record = requests.get(f"{person['api']}?format=json").json()

        if person_record is None and "email" in person.keys():
            person_record = self.lookup_sb_person_by_email(person["email"])

        if person_record is None:
            return None

        if "identifiers" in person_record.keys():
            person_identifiers = person_record["identifiers"]
        else:
            person_identifiers = list()

        the_orcid = None
        if "orcid" in person.keys():
            the_orcid = person["orcid"]
        else:
            the_orcid = person_record["orcId"]

        the_wikidata_id = None
        if "wikidata_id" in person.keys():
            the_wikidata_id = person["wikidata_id"]
        elif the_orcid is not None:
            name_list = [person_record["displayName"]]
            if "aliases" in person_record.keys():
                name_list.extend([n["name"] for n in person_record["aliases"]])

            the_wikidata_id = self.wd.lookup_wikidata_by_orcid(
                the_orcid,
                name_list=name_list
            )

        person_identifiers = self.package_identifiers(
            orcid=the_orcid,
            wikidata_id=the_wikidata_id,
            existing_ids=person_identifiers
        )

        if len(person_identifiers) > 0:
            person_record["identifiers"] = person_identifiers
            person_package = (person_record, True)
        else:
            person_package = (person_record, False)

        return person_package

    def update_sb_person_identifiers(self, person_packages):
        '''
        Works through a list of emails and executes update operations on them to update or insert identifiers into the
        person documents in the ScienceBase Directory in order to facilitate linked data operations. The function uses
        sciencebasepy to establish an authenticated session with requests in order to issue updates to the ScienceBase
        Directory API. This comes with a username and password prompt.

        The function will raise an error if there are invalid email address strings instead of continuing. It will also
        raise an error if any of the emails result in person records that can't be retrieved with
        get_identified_sb_person().

        :param person_packages: One or more dictionaries containing the necessary identifiers to update ScienceBase
        Directory person documents. Keys include a minimum of an email address from which all other processes will be
        triggered. Keys can also include the URL form of the ScienceBase Directory ID, orcid, and wikidata_id.
        :return: Python dictionary containing lists of person documents that were updated and/or not updated (because
        they didn't have anything able to be updated)
        '''
        if not isinstance(person_packages, list):
            person_packages = [person_packages]

        update_package = [self.identified_sb_person(person_package) for person_package in person_packages]

        update_person_records = [i[0] for i in update_package if i[1]]
        no_update_person_records = [i[0] for i in update_package if not i[1]]

        if len(update_person_records) > 0:

            try:
                sb = SbSession()
                sb.login(input("User Name: "), getpass("Password: "))
            except Exception as e:
                raise ValueError(f"Something went wrong in trying to authenticate to ScienceBase: {e}")

            try:
                for person in update_person_records:
                    put_link = person["link"]["href"]

                    sb._session.headers.update({'Content-Type': 'application/json'})
                    sb._session.put(
                        put_link,
                        data=json.dumps(person),
                        headers={
                            "content-type": "application/json",
                            "accept": "application/json"
                        }
                    )
            except Exception as e:
                raise ValueError(f"Something went wrong trying to send updates to ScienceBase: {e}")

        return {
            "updatedPersonRecords": update_person_records,
            "ignoredPersonRecords": no_update_person_records,
        }

    def package_identifiers(self, orcid=None, wikidata_id=None, existing_ids=list()):
        '''
        Packages an orcid in the manner ScienceBase Directory uses and then attempts to lookup a corresponding WikiData
        ID and packages that if found. This function is the key part of the process used in building out the identifiers
        in a ScienceBase person document.

        :param orcid: ORCID from the ScienceBase Person document
        :param wikidata_id: WikiData ID; supplied if already available through a separate process
        :param name_list: List of names (including aliases if available) to help make sure we get the right WikiData ID
        :param existing_ids: Any existing identifiers from the ScienceBase Person document
        :return: List of dictionaries in structure used by ScienceBase with type and key properties
        '''
        if len(existing_ids) > 0:
            # Get rid of the identifiers we are going to refactor, keeping anything else
            existing_ids = [i for i in existing_ids if "type" in i.keys() and i["type"] not in self.fix_identifiers]

        if orcid is not None:
            existing_ids.append(
                {
                    "type": "ORCID",
                    "key": orcid
                }
            )

        if wikidata_id is not None:
            if validators.url(wikidata_id):
                wikidata_id = wikidata_id.split('/')[-1]

            existing_ids.append(
                {
                    "type": "WikiData",
                    "key": wikidata_id
                }
            )

        return existing_ids

    def sb_people(self, filter=None, fields="default"):
        '''
        Uses the ScienceBase Directory search API to retrieve all person records with possible filters and field
        specifications. The main thing the SB Directory provides is a conduit to get all USGS staff via the sync with
        the DOI Active Directory as well as previous staff members as those are simply flagged inactive in SB. The
        Directory also contains non-USGS people who have become users at some point via myUSGS accounts. This function
        provides a helper to retrieve and structure these records for other uses.

        :param filter: Abstract filter options designed to pull back certain types of record sets for particular
        purposes
        :param fields: Abstract set of fields to simplify person documents coming from the SB Directory API for ease
        of use
        :return: List of dictionaries containing simplified attributes for person documents having an ORCID
        '''
        people = list()
        next_link = f"{self.sb_directory_people_api}?format=json&dataset=all&lq=_exists_:orcId&max=1000"

        while next_link is not None:
            data = requests.get(next_link).json()
            people.extend(data["people"])

            if "nextlink" in data.keys():
                next_link = data["nextlink"]["url"]
            else:
                next_link = None

        if fields == "default":
            return people
        elif fields == "simple":
            simple_people = [
                {
                    "uri": p["link"]["href"],
                    "displayName": p["displayName"],
                    "email": p["email"],
                    "identifiers": p["identifiers"]
                }
                for p in people
                if "identifiers" in p.keys()
                   and next((i for i in p["identifiers"] if i["type"] == "WikiData"), None) is not None
            ]

            return simple_people

    def update_person(self, sb_person):
        if isinstance(sb_person, str):
            sb_person = [sb_person]

        update_log = list()

        try:
            sb = SbSession()
            sb.login(input("User Name: "), getpass())
        except Exception as e:
            raise ValueError(f"Something went wrong in trying to authenticate to ScienceBase: {e}")

        for person in sb_person:
            put_link = person["link"]["href"]
            update_log.append(put_link)

            try:
                sb._session.headers.update({'Content-Type': 'application/json'})
                r = sb._session.put(
                    put_link,
                    data=json.dumps(person),
                    headers={
                        "content-type": "application/json",
                        "accept": "application/json"
                    }
                )
            except Exception as e:
                raise ValueError(f"Something went wrong trying to send updates to ScienceBase: {e}")

        return update_log

    def catalog_item_claims(self, sb_catalog_doc=None, sb_catalog_url=None):
        '''
        Extracts and formats potential associations from a ScienceBase Catalog items for use in knowledge graphing.
        :param sb_catalog_doc: ScienceBase Catalog item document
        :return: list of dictionaries containing concept associations derived from the catalog item
        '''
        if sb_catalog_doc is None and sb_catalog_url is not None:
            r = requests.get(sb_catalog_url, headers={"accept": "application/json"})
            if r.status_code == 200:
                sb_catalog_doc = r.json()

        if sb_catalog_doc is None:
            return None

        claims = list()

        date_qualifier = next(
            (
                i["dateString"] for i in sb_catalog_doc["dates"] if i["type"] == "Publication"
            ),
            None
        )
        if date_qualifier is None:
            date_qualifier = next(
                (
                    i["dateString"] for i in sb_catalog_doc["dates"] if i["type"] == "lastUpdated"
                ),
                None
            )

        claim_root = {
            "claim_created": datetime.utcnow().isoformat(),
            "claim_source": "ScienceBase Catalog",
            "reference": sb_catalog_doc["link"]["url"],
            "date_qualifier": date_qualifier,
            "subject_instance_of": "Person"
        }

        unique_contact_names = list(set([
            i["name"] for i in sb_catalog_doc["contacts"]
            if "contactType" in i.keys()
               and i["contactType"] == "person"
               and i["name"] not in self.ignore_org_names
        ]))

        for contact_name in unique_contact_names:
            contact = next((i for i in sb_catalog_doc["contacts"] if i["name"] == contact_name), None)

            contact_record = deepcopy(claim_root)
            if "oldPartyId" in contact.keys() and "contactType" in contact.keys():
                contact_record["subject_sbid"] = \
                    f"{self.sb_directory_base_url}{contact['contactType']}/{contact['oldPartyId']}"

            if "email" in contact.keys():
                contact_record["subject_email"] = contact["email"]

            if "orcid" in contact.keys():
                contact_record["subject_orcid"] = contact["orcid"]

            if "name" in contact.keys():
                contact_record["subject_label"] = contact["name"]

            if "jobTitle" in contact.keys():
                job_title_contact = deepcopy(contact_record)
                job_title_contact["property_label"] = "job title"
                job_title_contact["object_instance_of"] = "field of work"
                job_title_contact["object_label"] = contact["jobTitle"]
                claims.append(job_title_contact)

            if "organization" in contact.keys():
                org_contact = deepcopy(contact_record)
                org_contact["property_label"] = "organization affiliation"
                org_contact["object_instance_of"] = "organization"
                org_contact["object_label"] = contact["organization"]["displayText"]

                if "directoryId" in contact["organization"].keys():
                    org_contact["object_sbid"] = \
                        f"{self.sb_directory_base_url}organization/{contact['organization']['directoryId']}"
                claims.append(org_contact)

            if "tags" in sb_catalog_doc.keys():
                for tag in [
                    t for t in sb_catalog_doc["tags"] if "type" in t.keys()
                                                         and t["type"] not in ["Harvest Set"]
                ]:
                    tag_contact = deepcopy(contact_record)
                    tag_contact["property_label"] = "keyword"
                    tag_contact["object_instance_of"] = "data descriptor"
                    tag_contact["object_label"] = tag["name"]
                    tag_contact["object_qualifier"] = ":".join([v for k, v in tag.items() if k != "name"])
                    claims.append(tag_contact)

            for coauthor_name in [i for i in unique_contact_names if i != contact["name"]]:
                coauthor = next((i for i in sb_catalog_doc["contacts"] if i["name"] == coauthor_name), None)
                coauthor_contact = deepcopy(contact_record)
                coauthor_contact["property_label"] = "coauthor"
                if "type" in coauthor.keys():
                    coauthor_contact["object_qualifier"] = coauthor["type"]
                else:
                    coauthor_contact["object_qualifier"] = "data item co-listed contact"
                coauthor_contact["object_instance_of"] = "person"
                coauthor_contact["object_label"] = coauthor["name"]

                if "email" in coauthor.keys():
                    coauthor_contact["object_email"] = coauthor["email"]

                if "orcid" in coauthor.keys():
                    coauthor_contact["object_orcid"] = coauthor["orcid"]

                if "oldPartyId" in coauthor.keys():
                    coauthor_contact["object_sbid"] = \
                        f"{self.sb_directory_base_url}{coauthor['contactType']}/{coauthor['oldPartyId']}"
                claims.append(coauthor_contact)

            for org in [
                i for i in sb_catalog_doc["contacts"]
                if "contactType" in i.keys()
                   and i["contactType"] == "organization"
                   and i["name"] not in self.ignore_org_names
            ]:
                organization_contact = deepcopy(contact_record)
                organization_contact["property_label"] = "organization affiliation"
                organization_contact["object_instance_of"] = "organization"
                organization_contact["object_label"] = org["name"]
                if "oldPartyId" in org.keys():
                    organization_contact["object_sbid"] = \
                        f"{self.sb_directory_base_url}{org['contactType']}/{org['oldPartyId']}"
                claims.append(organization_contact)

        return claims

    def catalog_item_contacts(self, record_contacts, summary_record):
        contacts = list()
        for contact in record_contacts:
            contact_record = deepcopy(summary_record)
            try:
                del contact_record["abstract"]
                del contact_record["text"]
            except:
                pass
            contact_record["contact_name"] = contact["name"]
            contact_record["contact_type"] = "Unknown"
            contact_record["contact_role"] = "Unknown"
            contact_record["contact_email"] = None
            contact_record["contact_orcid"] = None
            contact_record["contact_sbid"] = None

            if "contactType" in contact.keys():
                contact_record["contact_type"] = contact["contactType"]

            if "type" in contact.keys():
                contact_record["contact_role"] = contact["type"]

            if "email" in contact.keys():
                contact_record["contact_email"] = contact["email"]

            if "orcid" in contact.keys():
                contact_record["contact_orcid"] = contact["orcid"]

            if "oldPartyId" in contact.keys():
                contact_record["contact_sbid"] = \
                    f"{self.sb_directory_base_url}{contact['contactType']}/{contact['oldPartyId']}"

            contacts.append(contact_record)

        return contacts

    def catalog_item_summary(self, sb_catalog_doc=None, sb_catalog_url=None, parse_sentences=False):
        '''
        Function to summarize the important pieces of information from a USGS Pubs Warehouse record for a given pub
        and normalize the related bits of information that we want to query on separately. We often want to do things
        like analyze authors, author affiliations, cost centers, and links separately as well as pull out the text
        parts useful in named entity recognition processes. This function provides that functionality off the live
        REST API response while we work to get a better data solution in place. This produces six separate data
        structures on a given batch of records that can be run in parallel. Each one is keyed on a new "uri" property
        that is a resolvable identifier for the publication (something else that is missing from the REST API).
        :param pw_record: Raw REST API response that can come from the API itself or a caching process that I've also
        employed to grab everything up in batches and stash into pickle files for later processing
        :return: summarized records as list of dictionaries, sentences tokenized from abstract and title for NER
        processing, USGS cost centers, authors, author affiliations, and links
        '''
        if sb_catalog_doc is None and sb_catalog_url is not None:
            r = requests.get(sb_catalog_url, headers={"accept": "application/json"})
            if r.status_code == 200:
                sb_catalog_doc = r.json()

        if sb_catalog_doc is None:
            return None

        summarized_record = dict()

        summarized_record["url"] = sb_catalog_doc["link"]["url"]
        summarized_record["name"] = sb_catalog_doc["title"]
        summarized_record["publisher"] = next((
            c["name"] for c in sb_catalog_doc["contacts"] if "type" in c and c["type"] == "Publisher"
        ), "U.S. Geological Survey")

        summarized_record["datecreated"] = next((
            d["dateString"] for d in sb_catalog_doc["dates"] if d["type"] == "dateCreated"
        ), datetime.utcnow().isoformat())

        summarized_record["datemodified"] = next((
            d["dateString"] for d in sb_catalog_doc["dates"] if d["type"] == "lastUpdated"
        ), summarized_record["datecreated"])

        summarized_record["datepublished"] = next((
            d["dateString"] for d in sb_catalog_doc["dates"] if d["type"] == "Publication"
        ), summarized_record["datecreated"])

        if "isaid_type" in sb_catalog_doc.keys():
            summarized_record["additionaltype"] = sb_catalog_doc["isaid_type"]
        else:
            summarized_record["additionaltype"] = "ScienceBase Catalog Item"

        if "identifiers" in sb_catalog_doc:
            summarized_record["identifier"] = next(
                (
                    i["key"] for i in sb_catalog_doc["identifiers"]
                    if "type" in i and i["type"] == "DOI"
                ), None)
        else:
            summarized_record["identifier"] = summarized_record["url"]

        record_sentences = list()
        if "body" in sb_catalog_doc.keys():
            processed_abstract = process_abstract(
                abstract=sb_catalog_doc["body"],
                title=summarized_record["name"],
                source_url=summarized_record["url"],
                parse_sentences=parse_sentences
            )

            summarized_record["abstract"] = processed_abstract["abstract"]
            record_sentences = processed_abstract["sentences"]

        links = list()
        if "distributionLinks" in sb_catalog_doc.keys():
            for link in sb_catalog_doc["distributionLinks"]:
                links.append({
                    "url": summarized_record["url"],
                    "link_url": link["uri"],
                    "link_type": link["typeLabel"],
                    "link_title": link["title"]
                })

        return {
            "assets": summarized_record,
            "sentences": record_sentences,
            "contacts": self.catalog_item_contacts(sb_catalog_doc["contacts"], summarized_record),
            "claims": self.catalog_item_claims(sb_catalog_doc=sb_catalog_doc),
            "links": links
        }

    def get_sb_org(self, identifier, map_it=True):
        if not validators.url(identifier):
            try:
                oldPartyId = int(identifier)
                identifier = f"{self.sb_directory_org_api}/{str(oldPartyId)}"
            except:
                return None

        r = requests.get(identifier, headers={"accept": "application/json"})
        
        if r.status_code != 200:
            return None
        
        if map_it:
            return bend(self.org_mapping, r.json())
        else:
            return r.json()


class Wikidata:
    def __init__(self):
        self.description = "Set of functions for interacting with the WikiData API"
        self.wikidata_endpoint = "https://query.wikidata.org/sparql"
        self.entity_data_root = "https://www.wikidata.org/wiki/Special:EntityData/"
        self.user_agent = f"pylinkedcmd/{sys.version_info[0]}.{sys.version_info[1]}"
        self.sparql = SPARQLWrapper(self.wikidata_endpoint, agent=self.user_agent)
        self.sparql.setReturnFormat(JSON)

    def get_wd_sparql_results(self, query):
        '''
        Runs a given SPARQL query and returns results

        :param query: SPARQL query string
        :return: WikiData SPARQL end point result set
        '''
        function_sparql = self.sparql
        function_sparql.setQuery(query)

        try:
            result_set = function_sparql.query().convert()
        except Exception as e:
            raise ValueError(f"SPARQL query could not be run - {e}")

        return result_set

    def lookup_wikidata_by_orcid(self, orc_id, name_list=list(), name_match_threshold=90):
        '''
        Uses the WikiData SPARQL service to look for an instanceOf human that is identified with a specified ORCID.
        If a list of names is supplied, we use that to help validate that a WikIData person record we find is probably
        the same person we are looking for by using token_set_ration from the fuzzywuzzy package with a configurable
        threshold. If there is more than one human identified with a given ORCID, we assume that more work needs to be
        done to disambiguate.

        :param orc_id: ORCID for a person we are checking on (no validation occurs on this string value)
        :param name_list: (default to blank list) List of names for the individual to help ensure proper match
        :param name_match_threshold: (default to 90) Threshold for name matching (based on Levenshtein distance between
        the name in WikiData and the supplied names)
        :return: WikiData identifier for a matched human entity in URL form or None if no match could be found
        '''
        query = """SELECT ?item ?itemLabel WHERE {
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
          ?item wdt:P496 '%s'.
          ?item wdt:P31 wd:Q5.
        }
        LIMIT 10""" % (orc_id)

        result_set = self.get_wd_sparql_results(query)

        if len(result_set["results"]["bindings"]) == 0:
            return None
        else:
            wikidata_id = result_set["results"]["bindings"][0]["item"]["value"]
            wikidata_name = result_set["results"]["bindings"][0]["itemLabel"]["value"]
            if len(name_list) > 0:
                name_similarity_check = [
                    (
                        name,
                        fuzz.token_set_ratio(wikidata_name, name)
                    ) for name in name_list
                ]
                probable_name_matches = [i[0] for i in name_similarity_check if i[1] > name_match_threshold]
                if len(probable_name_matches) == 0:
                    return None

            return wikidata_id

    def links_on_item(self, item_id):
        '''
        Runs a SPARQL query for all items that link to a particular item ID as either subject or object. Essentially,
        this results in the "what links here" that is provided via WikiData.

        :param item_id: Valid "Q" type identifier for a WikiData item
        :return: Returns a result set of subject, object, predicate triples for further processing or None if no
        results found
        '''
        query = """PREFIX wd: <http://www.wikidata.org/entity/>
        CONSTRUCT {
          wd:Q98058015 ?p1 ?o.
          ?s ?p2 wd:%s.
        }
        WHERE {
          {wd:%s ?p1 ?o.}
          UNION
          {?s ?p2 wd:%s.}
        }""" % (item_id, item_id, item_id)

        result_set = self.get_wd_sparql_results(query)

        if len(result_set["results"]["bindings"]) == 0:
            return None
        else:
            return result_set["results"]["bindings"]

    def wd_property(self, property_id):
        '''
        Looks up a WikiData property ("P" identifier) and returns a basic data structure useful for expressing
        "resolved" properties as item statements. The basic label and description information is useful in determining
        what the property may be used for. Properties have other useful properties that may need to be worked through
        over time. Initially, I am using this to get back the formatterUrl value that can be used to format a
        resolvable URL for certain types of identifiers on items.

        :param property_id: Valid "P" identifier for a property
        :return: Dictionary containing a simplified property document for further processing
        '''
        if property_id[:1] != "P":
            raise ValueError("Requires a 'P' identifier for a WikiData property")

        property_data = {
            "uri": f"{self.entity_data_root}{property_id}.json"
        }

        try:
            r_prop = requests.get(property_data["uri"]).json()
        except Exception as e:
            print(property_id, e)
            return None

        property_data["title"] = pydash.get(r_prop, f'entities.{property_id}.title')
        property_data["id"] = pydash.get(r_prop, f'entities.{property_id}.id')
        property_data["label_en"] = pydash.get(r_prop, f'entities.{property_id}.labels.en.value')
        property_data["description_en"] = pydash.get(r_prop, f'entities.{property_id}.descriptions.en.value')
        property_data["formatter_url"] = pydash.get(
            r_prop,
            f'entities.{property_id}.claims.P1630[0].mainsnak.datavalue.value'
        )

        return property_data

    def wd_item(
        self,
        entity_id,
        include_aliases=False,
        include_statements=True,
        include_raw_statements=False
    ):
        '''
        Builds a formatted document for a given WikiData item ("Q" identifier). There are many ways of accessing an
        item via SPARQL, APIs, and content negotiation. Getting to everything associated with an item in a way that
        does not require a massive number of queries has proven difficult. This is the best method I've come up with
        so far, but more experimentation is needed. This has mainly been tuned to retrieve information about people,
        but it can be used to return basic information about anything. The main area that will be variable for
        different kinds of items will be the statements/claims.

        :param entity_id: Valid "Q" identifier for an item/entity
        :param include_aliases: Include the aliases for an item or not
        :param include_statements: Process the statements/claims part of the item information or not
        :param include_raw_statements: If processing the statements, include the raw statement data or not
        (Additional work is needed on specialized processing methods for particular types of properties. It's useful to
        be able to see the raw results for working through these.)
        :return: Dictionary containing high level attributes for an item, focusing on English language label, etc. and
        an array of statements processed from the claims.
        '''
        if entity_id[:1] != "Q":
            raise ValueError("Requires a 'Q' identifier for a WikiData item")

        entity_data = {
            "uri": f"{self.entity_data_root}{entity_id}.json"
        }

        try:
            r_entity = requests.get(entity_data["uri"]).json()
        except Exception as e:
            print(entity_id, e)
            return None

        entity_data["lastrevid"] = pydash.get(r_entity, f"entities.{entity_id}.lastrevid")
        entity_data["modified"] = pydash.get(r_entity, f"entities.{entity_id}.modified")
        entity_data["label_en"] = pydash.get(r_entity, f"entities.{entity_id}.labels.en.value")
        entity_data["description_en"] = pydash.get(r_entity, f"entities.{entity_id}.descriptions.en.value")

        if include_aliases and bool(r_entity["entities"][entity_id]["aliases"]):
            entity_data["aliases"] = [i["value"] for i in r_entity["entities"][entity_id]["aliases"]["en"]]

        if include_statements:
            entity_data["statements"] = list()

            for key, value in pydash.get(r_entity, f"entities.{entity_id}.claims").items():
                property_attributes = self.wd_property(key)
                property_attributes["url"] = None

                for instance in value:
                    this_instance_attributes = copy.deepcopy(property_attributes)

                    if include_raw_statements:
                        this_instance_attributes["raw_data"] = instance

                    property_value = pydash.get(instance, "mainsnak.datavalue.value")

                    if isinstance(property_value, str):
                        this_instance_attributes["value"] = property_value
                    elif isinstance(property_value, dict) \
                        and "entity-type" in property_value.keys() \
                            and property_value["entity-type"] == "item":
                        resolved_prop_value = self.wd_item(property_value["id"], include_statements=False)
                        this_instance_attributes["value"] = resolved_prop_value["label_en"]
                        this_instance_attributes["url"] = f"{self.entity_data_root}{property_value['id']}"
                    elif isinstance(property_value, dict) \
                        and "time" in property_value.keys():
                        this_instance_attributes["value"] = property_value["time"]
                    elif isinstance(property_value, dict) \
                        and "text" in property_value.keys():
                        this_instance_attributes["value"] = property_value["text"]

                    if this_instance_attributes["formatter_url"] is not None:
                        this_instance_attributes["url"] = this_instance_attributes["formatter_url"].replace(
                            "$1",
                            this_instance_attributes["value"]
                        )

                    entity_data["statements"].append(this_instance_attributes)

        return entity_data

    def get_authored_articles(self, author_id, return_result="full"):
        '''
        Runs a SPARQL query to look for anything authored by a WikiData item (usually a person but it only looks for
        authored by the ID provided). Will either return just the identifiers for the items found or will run the
        function to collect item properties on those IDs to return a useful recordset.

        :param author_id: Valid 'Q' identifier for a WikiData item
        :param return_result: Either "full" or "ids" to specify the return type
        :return: Either list of identifier strings or list of dictionaries from the wd_item_properties
        '''
        if author_id[:1] != "Q":
            raise ValueError("author_id must be a valid 'Q' identifier")

        query = """SELECT ?article ?articleLabel 
            WHERE {
              ?article wdt:P50 wd:%s.
              SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
            }""" % (author_id)

        results = self.get_wd_sparql_results(query)

        article_id_list = [i["article"]["value"].split("/")[-1] for i in results["results"]["bindings"]]

        if return_result == "full":
            article_result = self.wd_item_properties(article_id_list, return_result="values")
        else:
            article_result = article_id_list

        return article_result

    def wd_item_properties(self, items, return_result="raw"):
        '''
        Runs a SPARQL query to collect all properties and property values for a list of WikiData Items. Will format
        them by stripping out to just ids, property names, and property values if requested.

        :param items: List of WikiData items (valid 'Q' identifiers)
        :param return_result: Either raw by default or 'values' if you want just the simple label values
        :return: List of dictionaries containing either simple values or raw query results
        '''
        items_string = ' '.join(f"wd:{i}" for i in items)

        query = """SELECT ?itemName ?wdLabel ?ps_Label {
          VALUES ?itemName {%s}
          ?itemName ?p ?statement .
          ?statement ?ps ?ps_ .

          ?wd wikibase:claim ?p.
          ?wd wikibase:statementProperty ?ps.

          SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
        } ORDER BY ?wd ?statement ?ps_""" % (items_string)

        results = self.get_wd_sparql_results(query)

        if return_result == "values":
            property_data = list()
            for item_prop in results["results"]["bindings"]:
                property_data.append(
                    {
                        "item_id": item_prop["itemName"]["value"].split("/")[-1],
                        "property": item_prop["wdLabel"]["value"],
                        "value": item_prop["ps_Label"]["value"]
                    }
                )
            return_results = property_data
        else:
            return_results = results["results"]["bindings"]

        return return_results

    def entity_data(self, entity_id, claims=True):
        entity_response = requests.get(f"{self.entity_data_root}{entity_id}.json")
        if entity_response.status_code != 200:
            return None, None

        entity_response_data = entity_response.json()

        if entity_id not in entity_response_data["entities"].keys():
            return None, None

        entity_doc = entity_response_data["entities"][entity_id]
        entity_record = dict()
        for k, v in entity_doc.items():
            if isinstance(v, str):
                entity_record[k] = v

        try:
            entity_record["label_en"] = entity_doc["labels"]["en"]["value"]
        except:
            pass

        try:
            entity_record["description_en"] = entity_doc["descriptions"]["en"]["value"]
        except:
            pass

        try:
            entity_record["aliases_en"] = [i["value"] for i in entity_doc["aliases"]["en"]]
        except:
            pass

        entity_claims = None

        if claims and "claims" in entity_doc.keys():
            entity_claims = list()
            for item in entity_doc["claims"].items():
                mainsnak = item[1][0]["mainsnak"]
                property_id = mainsnak["property"]
                if mainsnak["datavalue"]["type"] == "string":
                     property_value = mainsnak["datavalue"]["value"]
                elif mainsnak["datavalue"]["type"] == "wikibase-entityid":
                    property_value = mainsnak["datavalue"]["value"]["id"]
                elif mainsnak["datavalue"]["type"] == "time":
                    property_value = mainsnak["datavalue"]["value"]["time"]
                else:
                    property_value = json.dumps(mainsnak["datavalue"]["value"])

                entity_claims.append({
                    "entity_id": entity_id,
                    "property_id": property_id,
                    "property_value": property_value
                })

        return entity_record, entity_claims


class Isaid:
    def __init__(self):
        self.description = "Set of functions for working with iSAID - the integrated Science Assessment Information" \
                           "Database."
        self.isaid_api = "https://isaid-data.datadistillery.org/v1/graphql"
        self.api_headers = {
            "content-type": "application/json",
        }
        self.isaid_data_collections = {
            "directory": {
                "title": "Directory",
                "description": "Properties pulled from the ScienceBase Directory for USGS employees and other people"
            },
            "assets": {
                "title": "Assets",
                "description": "Scientific assets such as publications, datasets, models, instruments, and "
                               "other articles. Linked to people through roles such as author/creator."
            },
            "claims": {
                "title": "Claims",
                "description": "Statements or assertions about a person or asset that characterize the entities in "
                               "various ways, including the connections between entities."
            },
            "wikidata_entities": {
                "title": "WikiData Entity",
                "description": "An entity in WikiData representing a person or scientific asset."
            },
            "wikidata_claims": {
                "title": "WikiData Claims",
                "descriptions": "Statements or assertions about a person or other entity in WikiData that characterize "
                                "them in various ways, including the connections between entities."
            }
        }

    def evaluate_criteria_people(self, criteria, parameter=None):
        '''
        Builds where clause criteria for GraphQL queries for people. Will detect common search patterns for identifier-
        based searches but otherwise requires the parameter to be explicitly supplied.
        :param criteria: search criteria can be either a list of strings or a single string
        :param parameter: one of a set of available query parameters
        :return: where string to be included in GraphQL query
        '''
        if isinstance(criteria, list):
            sample_criteria = criteria[0]
            query_operator = "_in"
        elif isinstance(criteria, str):
            sample_criteria = criteria
            query_operator = "_eq"
        else:
            raise ValueError("You must supply either a list of values or a single string value as criteria.")

        if not isinstance(sample_criteria, str):
            raise ValueError("You must supply either a list of values or a single string value as criteria.")

        if parameter is not None:
            valid_parameters = [
                "url",
                "state",
                "professionalQualifier",
                "personalTitle",
                "organization_uri",
                "organization_name",
                "middleName",
                "lastName",
                "jobTitle",
                "identifier_wikidata",
                "identifier_orcid",
                "identifier_email",
                "identifier_sb_uri",
                "generationalQualifier",
                "firstName",
                "email",
                "displayName",
                "description",
                "date_cached",
                "city",
                "cellPhone"
            ]

            if parameter not in valid_parameters:
                raise ValueError(f"Your query parameter must be in the following list: {str(valid_parameters)}")

            query_parameter = parameter
        else:
            if validators.email(sample_criteria):
                query_parameter = "identifier_email"
            elif validators.url(sample_criteria):
                if sample_criteria.split("/")[4] == "organization":
                    query_parameter = "organization_uri"
                elif sample_criteria.split("/")[4] == "person":
                    query_parameter = "identifier_sbid"
                else:
                    raise ValueError("Criteria contains a URL, but it's not one that can be queried with.")
            else:
                if re.match(r"\d{4}-\d{4}-\d{4}-\d{4}", sample_criteria):
                    query_parameter = "identifier_orcid"
                elif re.match(r"Q\d*", sample_criteria):
                    query_parameter = "identifier_wikidata"
                else:
                    raise ValueError(
                        "Criteria contains a string, but it's not recognized as a particular type. "
                        "Please supply a parameter name.")

        if isinstance(criteria, list):
            string_criteria = str(criteria).replace("'", '"')
        else:
            string_criteria = f'"{criteria}"'

        where_criteria = '(where: {%s: {%s: %s}})' % (
            query_parameter,
            query_operator,
            string_criteria
        )

        if query_parameter.split("_")[0] != "identifier":
            people_records = self.get_people(where_clause=where_criteria)
            where_criteria = '(where: {%s: {%s: %s}})' % (
                "identifier_sbid",
                "_in",
                str([i["identifier_sbid"] for i in people_records]).replace("'", '"')
            )

        return where_criteria

    def execute_query(self, query, api=None):
        '''
        Executed query against GraphQL end point
        :param query: GraphQL formatted query statement
        :return: GraphQL response
        '''
        if api is None:
            api = self.isaid_api

        r = requests.post(
            api,
            json={"query": query},
            headers=self.api_headers,
            verify=False
        )

        if r.status_code != 200:
            raise ValueError("Query could not be processed.")

        return r.json()

    def get_people(self, criteria=None, parameter=None, where_clause=None):
        if where_clause is None:
            where_clause = str()

            if criteria is not None:
                try:
                    where_clause = self.evaluate_criteria_people(criteria, parameter)
                except ValueError as e:
                    return e

        q = '''
        {
            directory %(where_clause)s {
                identifier_sbid
                identifier_email
                identifier_orcid
            }
        }
        ''' % {"where_clause": where_clause}
        try:
            query_response = self.execute_query(q)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            return query_response["data"]["directory"]

    def people_by_org(self, organization_name, response="email_list"):
        where_clause = '''
        (where: {organization_name: {_eq: "%s"}})
        ''' % (organization_name)

        q = '''
        {
            directory %(where_clause)s {
                identifier_email
                identifier_sbid
                identifier_wikidata
                identifier_orcid
                jobtitle
                lastname
                middlename
                note
                orcid
                organization_name
                organization_uri
                personaltitle
                professionalqualifier
                state
                url
                generationalqualifier
                firstname
                email
                displayname
                description
                date_cached
                city
                cellphone
            }
        }
        ''' % {"where_clause": where_clause}
        try:
            query_response = self.execute_query(q)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            print(query_response)
            return None
        else:
            if response == "email_list":
                data_response = [i["identifier_email"] for i in query_response["data"]["directory"]]
                data_response.sort()

            return data_response

    def assemble_person_record(self, criteria, parameter="identifier_email", datatypes=None):
        if parameter.split("_")[0] != "identifier":
            raise ValueError("")

        if datatypes is None:
            datatypes = [k for k, v in self.isaid_data_collections.items()]
        else:
            datatypes = [i for i in datatypes if i in [k for k, v in self.isaid_data_collections.items()]]

        try:
            where_clause = self.evaluate_criteria_people(criteria, parameter)
        except ValueError as e:
            return e

        query_sections = dict()

        query_sections["directory"] = '''
            directory %(where_clause)s {
                cellphone
                city
                date_cached
                description
                displayname
                email
                firstname
                generationalqualifier
                identifier_email
                identifier_orcid
                identifier_sbid
                identifier_wikidata
                jobtitle
                lastname
                middlename
                note
                organization_name
                organization_uri
                organization_url
                region
                usgs_mission_areas
                usgs_programs
                personaltitle
                professionalqualifier
                state
                url
            }
        ''' % {"where_clause": where_clause}

        query_sections["assets"] = '''
            assets %(where_clause)s {
                additionaltype
                contact_role
                contact_type
                datecreated
                datemodified
                datepublished
                identifier_email
                identifier_orcid
                identifier
                identifier_sbid
                identifier_wikidata
                name
                publication
                publisher
                url
            }
        ''' % {"where_clause": where_clause}

        query_sections["claims"] = '''
            claims %(where_clause)s {
                claim_created
                claim_source
                date_qualifier
                object_instance_of
                object_label
                object_qualifier
                property_label
                reference
                subject_instance_of
                subject_label
                subject_identifier_email
                subject_identifier_orcid
                subject_identifier_sbid
                subject_identifier_wikidata
                object_identifier_email
                object_identifier_orcid
                object_identifier_sbid
                object_identifier_wikidata
            }
        ''' % {"where_clause": where_clause.replace("identifier_", "subject_identifier_")}

        query_sections["wikidata_entities"] = '''
            wikidata_entities %(where_clause)s {
                aliases_en
                description_en
                identifier_wikidata
                label_en
                modified
                type
            }
        ''' % {"where_clause": where_clause}

        query_sections["wikidata_claims"] = '''
            wikidata_claims %(where_clause)s {
                property_description
                property_entity_description
                property_entity_label
                property_id
                property_label
                property_value
            }
        ''' % {"where_clause": where_clause}

        query = '''
        {
        '''

        for data_type in datatypes:
            query = '''
            %s
            %s
            ''' % (query, query_sections[data_type])

        query = '''
        %s
        }
        ''' % (query)

        try:
            query_response = self.execute_query(query)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            return query_response["data"]

    def get_organizations(self):
        q_orgs = '''
            {
              directory (distinct_on: organization_uri, where: {organization_uri: {_is_null: false}}) {
                organization_name
                organization_uri
              }
            }
        '''
        try:
            query_response = self.execute_query(q_orgs)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            return query_response["data"]["directory"]


def process_abstract(abstract, source_url, title=None, parse_sentences=False):
    abstract_soup = BeautifulSoup(abstract, 'html.parser')
    abstract_text = abstract_soup.get_text()

    sentences = list()
    if parse_sentences:
        if title is not None:
            sentences.append({
                "url": source_url,
                "source": "title",
                "sentence": title.strip()
            })

        for sentence in sent_tokenize(abstract_text):
            sentences.append({
                "url": source_url,
                "source": "abstract",
                "sentence": sentence
            })

    return {
        "abstract": abstract_text,
        "sentences": sentences
    }

def exists(obj, chain):
    _key = chain.pop(0)
    if _key in obj:
        return exists(obj[_key], chain) if chain else obj[_key]

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

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]