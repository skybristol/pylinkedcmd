import requests
from datetime import datetime
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
from collections import Counter


class Sciencebase:
    def __init__(self):
        self.description = "Set of functions for working with the ScienceBase Directory"
        self.sb_directory_people_api = "https://www.sciencebase.gov/directory/people"
        self.sb_directory_org_api = "https://www.sciencebase.gov/directory/organization"
        self.fix_identifiers = ["ORCID", "WikiData"]
        self.wd = Wikidata()

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
            "uri": person_doc["link"]["href"],
            "date_cached": datetime.utcnow().isoformat()
        }
        for k, v in person_doc.items():
            if k not in ignore_props and isinstance(v, str):
                new_person_doc[k] = v

        if "identifiers" in person_doc.keys():
            for i in person_doc["identifiers"]:
                new_person_doc[f"identifier_{i['type']}"] = i["key"]

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

        return new_person_doc

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
                sb.login(input("User Name: "), getpass())
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
        entity_response = requests.get(f"{self.entity_data_root}{entity_id}.json").json()
        entity_doc = entity_response["entities"][entity_id]
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


class UsgsWeb:
    def __init__(self):
        self.usgs_pro_page_listing = "https://www.usgs.gov/connect/staff-profiles"
        self.expertise_link_pattern = re.compile(r"^\/science-explorer-results\?*")
        self.profile_link_pattern = re.compile(r"^\/staff-profiles\/*")
        self.orcid_link_pattern = re.compile(r"^https:\/\/orcid.org\/*")
        self.mailto_link_pattern = re.compile(r"^mailto:")
        self.tel_link_pattern = re.compile(r"^tel:")
        self.org_link_pattern = re.compile(r"www.usgs.gov")
        self.usgs_web_root = "https://usgs.gov"

    def get_staff_inventory_pages(self, title_="Go to last page"):
        '''
        Unfortunately, the only way to get the entire staff inventory as presented on the USGS web that I've found is to
        iterate over every page in the closed web system and scrape listings. To figure out what pages are contained in
        the listing without filters, we need to consult the first page and get a page number for the last listing from
        a particular link. This function handles that process and gives us back every URL we need to hit.
        :param title_: title of the link pointing to the last page of the inventory
        :type title_: str
        :return: list of URLs to every page comprising the entire inventory of USGS staff
        '''
        r = requests.get(self.usgs_pro_page_listing)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.content, 'html.parser')

        last_page_num = soup.find('a', title=title_)["href"].split("=")[-1]

        inventory_url_list = [
            f"{self.usgs_pro_page_listing}?page={n}" for n in range(0, int(last_page_num)+1)
        ]

        return inventory_url_list

    def get_staff_listing(self, page_url, tag_="div", class_="views-column"):
        '''
        This function handles the process of fetching a given page from the staff listing inventory, extracting the
        useful records and returning a listing.
        :param page_url: URL to a specific staff listing page
        :type page_url: str
        :param tag_: HTML tag that combines with a class name to identify the part of the HTML document containing the
        relevant listing sections
        :type tag_: str
        :param class_: name of the CSS class in the HTML document that indicates the relevant sections to process
        :type class_: str
        :return: list of dictionaries containing name, email, and profile from the process_staff_section function for
        each person record found in the specified sections
        '''
        r = requests.get(page_url)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.content, 'html.parser')

        page_staff_listing = list()

        for section in soup.findAll(tag_, class_=class_):
            page_staff_listing.append(self.process_staff_section(section))

        return page_staff_listing

    def process_staff_section(self, section, email_in_common=["ask@usgs.gov"]):
        '''
        Unfortunately, none of the accessible directory sources for USGS personnel seem to have the link to USGS
        staff profile pages. The only location that I can find these is through the USGS web page at
        https://www.usgs.gov/connect/staff-profiles, and the only way to get at those data is through a web scraper.
        This function handles the process of extracting a usable data structure from the section on the pages that
        contain individual person listing.
        :param section: a BeautifulSoup4 data object containing the div for a given staff person listing from which we
        need to extract useful information
        :type section: bs4.element.Tag
        :return: dictionary containing the name, email, and profile (URL) for a person (email and profile will be
        returned with None if not found in the record
        '''
        profile_page_link = section.find("a", href=self.profile_link_pattern)
        email_link = section.find("a", href=self.mailto_link_pattern)
        tel_link = section.find("a", href=self.tel_link_pattern)
        org_link = section.find("a", href=self.org_link_pattern)

        person_record = {
            "name": None,
            "title": None,
            "organization_name": None,
            "organization_link": None,
            "email": None,
            "profile": None,
            "telephone": None
        }

        if profile_page_link is not None:
            person_record["name"] = profile_page_link.text.replace("\t", "").strip()
            person_record["profile"] = f'{self.usgs_web_root}{profile_page_link["href"]}'
        else:
            person_record["profile"] = None
            name_container = section.find("h4", class_="field-content")
            if name_container is not None:
                person_record["name"] = name_container.text.replace("\t", "")

        if org_link is not None:
            person_record["organization_name"] = org_link.text.replace("\t", "").strip()
            person_record["organization_link"] = org_link["href"]

        if email_link is not None:
            person_record["email"] = email_link.text.replace("\t", "").strip()

        if tel_link is not None:
            person_record["telephone"] = tel_link.text.replace("\t", "").strip()

        bolded_item = section.find("b")
        if bolded_item is not None:
            person_record["title"] = bolded_item.text.replace("\t", "").strip()

        if person_record["email"] is not None and person_record["email"] not in email_in_common:
            person_record["identifier"] = person_record["email"]
        elif person_record["email"] in email_in_common and person_record["profile"] is not None:
            person_record["identifier"] = person_record["profile"]
        else:
            person_record["identifier"] = person_record["name"]

        return person_record

    def scrape_profile(self, page_url):
        '''
        Unfortunately, there is no current programmatic way of getting at USGS staff profile pages, where at least some
        staff have put significant effort into rounding out their available online information. For some, these pages
        represent the best personally managed location to get at published works and other details. For now, the most
        interesting bits from these pages include a self-asserted list of "expertise" keywords drawn from the USGS
        Thesaurus along with a body section containing variable content. This function collects the expertise keywords
        for further analysis, pulls out links from the body (which can be compared with other sources), and shoves the
        body text as a whole into the data for further processing.
        :param page_url: URL to the profile page that can be used as a unique key
        :return: dictionary containing the url, list of expertise keywords (if available), list of links (text and
        href) values in dictionaries, and the full body html as a string
        '''
        r = requests.get(page_url)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.content, 'html.parser')

        profile_page_data = {
            "profile": page_url,
            "date_cached": datetime.utcnow().isoformat(),
            "display_name": None,
            "profile_image_url": None,
            "organization_name": None,
            "organization_link": None,
            "email": None,
            "orcid": None,
            "body_content_links": list(),
        }

        expertise_section = soup.find("section", class_="staff-expertise")
        if expertise_section is not None:
            profile_page_data["expertise"] = [
                t.text for t in expertise_section.findAll("a", href=self.expertise_link_pattern)
            ]

        profile_body_content = soup.find("div", class_="usgs-body")
        if profile_body_content is not None:
            profile_page_data["scraped_body_html"] = str(profile_body_content)
            link_list = profile_body_content.findAll("a")
            if link_list is not None:
                for link in link_list:
                    try:
                        profile_page_data["body_content_links"].append({
                            "link_text": link.text,
                            "link_href": link["href"]
                        })
                    except Exception as e:
                        print(e)
                        continue


        display_name_container = soup.find("div", class_="full-width col-sm-12")
        if display_name_container is not None:
            display_name_container_inner = display_name_container.find("h1", class_="page-header")
            if display_name_container_inner is not None:
                profile_page_data["display_name"] = display_name_container_inner.text

        email_container = soup.find("div", class_="email")
        if email_container is not None:
            email_link = email_container.find("a", href=self.mailto_link_pattern)
            if email_link is not None:
                profile_page_data["email"] = email_link.text

        organization_container = soup.find("h3", class_="staff-profile-subtitle h4")
        if organization_container is not None:
            organization_link_container = organization_container.find("a")
            if organization_link_container is not None:
                profile_page_data["organization_link"] = organization_link_container["href"]
                profile_page_data["organization_name"] = organization_link_container.text

        profile_image = soup.find("img", class_='staff-profile-image')
        if profile_image is not None:
            profile_page_data["profile_image_url"] = profile_image["src"]

        orcid_link = soup.find("a", href=self.orcid_link_pattern)
        if orcid_link is not None:
            profile_page_data["orcid"] = orcid_link.text.split("/")[-1]

        other_pubs_container = soup.find(
            "div",
            class_="entity entity-field-collection-item field-collection-item-field-non-usgs-publication clearfix"
        )
        if other_pubs_container is not None:
            profile_page_data["body_content_links"].extend([
                {
                    "link_text": l.text,
                    "link_href": l["href"]
                } for l in other_pubs_container.findAll("a")
            ])

        return profile_page_data

    def get_unique_staff(self, staff_list, return_type="dict"):
        df = pd.DataFrame([dict(t) for t in {tuple(d.items()) for d in staff_list}])

        df_grouped_staff = df.groupby('identifier').agg(
            {
                'profile': lambda x: list(set(list(x))),
                'email': lambda x: list(set(list(x))),
                'name': lambda x: list(set(list(x))),
                'organization_name': lambda x: list(set(list(x))),
                'organization_link': lambda x: list(set(list(x))),
                'telephone': lambda x: list(set(list(x))),
                'title': lambda x: list(set(list(x)))
            }
        ).reset_index()

        if return_type == "dict":
            return df_grouped_staff.to_dict(orient="records")
        else:
            return df_grouped_staff


class Pw:
    def __init__(self):
        self.description = "Set of functions for working with the USGS Pubs Warehouse REST API a little more better"
        self.publication_api = "https://pubs.er.usgs.gov/pubs-services/publication"

    def get_pw_query_urls(self, year):
        '''
        Function helps to build out logical batches of USGS Pubs Warehouse REST API queries in order to retrieve all
        records and cache elsewhere for more usability.
        :param year: 4 digit year to build 1000 record batches for
        :return: List of dictionaries containing the year, total record count, the URL for the query, and the date
        the URL reference was determined
        '''
        pw_retrieval_list = list()

        pw_url = f"{self.publication_api}?startYear={year}&endYear={year}"
        pw_result = requests.get(f"{pw_url}&pageSize=1").json()

        if pw_result["recordCount"] > 0:
            for page_num in range(1, math.ceil(int(pw_result["recordCount"]) / 1000) + 1):
                pw_retrieval_list.append({
                    "year": year,
                    "record_count": pw_result["recordCount"],
                    "url": f"{pw_url}&page_size=1000&page_number={page_num}",
                    "date_cached": datetime.utcnow().isoformat()
                })
            return pw_retrieval_list

    def summarize_pw_record(self, pw_record):
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
        summarized_record = dict()
        record_sentences = list()

        summarized_record["uri"] = f"{self.publication_api}/{pw_record['text'].split('-')[0].strip()}"
        summarized_record["pw_id"] = pw_record["id"]
        summarized_record["publicationYear"] = pw_record["publicationYear"]
        summarized_record["title"] = pw_record["title"]
        summarized_record["meta_text"] = pw_record["title"]
        summarized_record["lastModifiedDate"] = pw_record["lastModifiedDate"]
        summarized_record["summary_created"] = datetime.utcnow().isoformat()

        record_sentences.append({
            "uri": summarized_record["uri"],
            "source": "title",
            "sentence": summarized_record["title"]
        })

        if "docAbstract" in pw_record.keys():
            abstract_soup = BeautifulSoup(pw_record["docAbstract"], 'html.parser')
            if abstract_soup.get_text() != "No abstract available.":
                summarized_record["abstract"] = abstract_soup.get_text()
                summarized_record["meta_text"] = f"{summarized_record['meta_text']}. {summarized_record['abstract']}"
                for sentence in sent_tokenize(summarized_record["abstract"]):
                    record_sentences.append({
                        "uri": summarized_record["uri"],
                        "source": "abstract",
                        "sentence": sentence
                    })

        try:
            summarized_record["publisher"] = pw_record["publisher"]
        except KeyError:
            pass

        try:
            summarized_record["doi"] = pw_record["doi"]
        except KeyError:
            pass

        try:
            summarized_record["publicationType"] = pw_record["publicationType"]["text"]
            summarized_record["publicationSubtype"] = pw_record["publicationSubtype"]["text"]
            summarized_record["seriesTitle"] = pw_record["seriesTitle"]["text"]
        except KeyError:
            pass

        try:
            summarized_record["scienceBaseUri"] = pw_record["scienceBaseUri"]
        except KeyError:
            pass

        if "costCenters" in pw_record.keys():
            cost_centers = [
                dict(
                    item,
                    **{
                        'uri': summarized_record["uri"],
                        'publicationYear': pw_record["publicationYear"]
                    }
                ) for item in pw_record["costCenters"]
            ]
        else:
            cost_centers = None

        if "contributors" in pw_record.keys() and "authors" in pw_record["contributors"].keys():
            authors = [
                dict(
                    item,
                    **{
                        'uri': summarized_record["uri"],
                        'publicationYear': pw_record["publicationYear"]
                    }
                ) for item in pw_record["contributors"]["authors"]
            ]
        else:
            authors = None

        if authors is not None:
            affiliations = list()
            for author in [a for a in authors if "affiliations" in a.keys() and len(a["affiliations"]) > 0]:
                for affiliation in author["affiliations"]:
                    affiliation["author_text"] = author["text"]
                    affiliation["uri"] = author["uri"]
                    affiliation["publicationYear"] = author["publicationYear"]
                    if "orcid" in author.keys():
                        affiliation["orcid"] = author["orcid"]
                    if "email" in author.keys():
                        affiliation["email"] = author["email"]
                    affiliations.append(affiliation)
        else:
            affiliations = None

        if "links" in pw_record.keys() and len(pw_record["links"]) > 0:
            links = list()
            for link in pw_record["links"]:
                links.append({
                    "uri": summarized_record["uri"],
                    "link_url": link["url"],
                    "link_type": link["type"]["text"]
                })
        else:
            links = None

        return summarized_record, record_sentences, cost_centers, authors, affiliations, links


class Isaid:
    def __init__(self):
        self.description = "Set of functions for working with iSAID - the integrated Science Assessment Information" \
                           "Database."
        self.isaid_api = "https://fast-iguana-60.hasura.app/v1/graphql"
        self.api_headers = {
            "content-type": "application/json",
        }

    def execute_query(self, query):
        '''
        Executed query against GraphQL end point
        :param query: GraphQL formatted query statement
        :return: GraphQL response
        '''
        r = requests.post(
            self.isaid_api,
            json={"query": query},
            headers=self.api_headers
        )

        if r.status_code != 200:
            raise ValueError("Query could not be processed.")

        return r.json()

    def lookup_person(self, identifier, parameter="email"):
        '''
        The intent is to cache one and only one record for a given person in the iSAID database that is uniquely
        and persistently identified by several different identifier schemes. This function takes one of those
        identifiers (defaulting to email as the most common inbound search vector at this time) and returns a match.
        :param identifier: Identifier value to uniquely identify individual person
        :param parameter: Variable containing the identifier; should be one of email, identifier_ORCID,
        identifier_WikiData, or uri (ScienceBase Directory)
        :return: GraphQL result (JSON object/dictionary) containing the primary identifying attributes from the
        ScienceBase Directory cached in the iSAID database (returns None if no results found or API query fails)
        '''
        q_sb = '''
          {
            sb_usgs_employees(where: {%s: {_eq: "%s"}}) {
                url
                uri
                state
                professionalQualifier
                personalTitle
                organization_uri
                organization_name
                note
                middleName
                lastName
                jobTitle
                identifier_WikiData
                identifier_ORCID
                generationalQualifier
                firstName
                email
                displayName
                description
                date_cached
                city
                cellPhone
            }
          }
        ''' % (parameter, identifier)
        try:
            query_response = self.execute_query(q_sb)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            if len(query_response["data"]["sb_usgs_employees"]) > 1:
                return "Query returned more than one record, and only one record was expected."

            return query_response["data"]["sb_usgs_employees"][0]

    def lookup_expertise(self, identifier, parameter="email"):
        '''
        Looks up and returns expertise terms from USGS profile pages scraped and cached in the iSAID database
        :param identifier: Identifier value to uniquely identify individual person
        :param parameter: Variable containing the identifier; should be one of email, identifier_ORCID,
        identifier_WikiData, or uri (ScienceBase Directory); may also use source_identifier with the URL for the
        Profile Page, but this can vary in format depending on how it was populated into the ScienceBase Directory
        :return: GraphQL result (JSON array of objects/list of dictionaries) containing the expertise terms cached
        from the USGS profile page (returns None if no results found or API query fails)
        '''
        q_expertise = '''
          {
            identified_expertise(where: {%s: {_eq: "%s"}}) 
            {
              term
            }
          }
        ''' % (parameter, identifier)
        try:
            query_response = self.execute_query(q_expertise)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            return query_response["data"]["identified_expertise"]

    def lookup_pubs(self, identifier, parameter="email"):
        '''
        Looks up and returns publication records from the USGS Publications Warehouse cached in the iSAID database for
        a given author. At this time, this will only return publications where the PW records have been properly
        cataloged with identifiers on authors (either email or ORCID). We need to verify how extensive this cataloging
        is on the Pubs Warehouse side and probably include either functionality for additional processing into the
        cache or else a more sophisticated lookup mechanism.
        :param identifier: Identifier value to uniquely identify individual author
        :param parameter: Variable containing the identifier; should be one of email, identifier_ORCID,
        identifier_WikiData, or uri (ScienceBase Directory)
        :return: GraphQL result (JSON array of objects/list of dictionaries) containing the uri, title, and year of
        publication from the PW cache (returns None if no results found or API query fails)
        '''
        q_pubs = '''
          {
            identified_pw_authors(where: {%s: {_eq: "%s"}})
            {
              title
              uri
              publicationYear
            }
          }
        ''' % (parameter, identifier)
        try:
            query_response = self.execute_query(q_pubs)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            return query_response["data"]["identified_pw_authors"]

    def lookup_co_authors(self, pub_list, identifier, parameter="email"):
        '''
        Given a list of publication URIs, returns the list of co-authors from all publications. Uses an identifier for
        the subject author to filter.
        :param pub_list: List of publication URL URIs
        :param identifier: Identifier value to uniquely identify individual author
        :param parameter: Variable containing the identifier; should be one of email, identifier_ORCID,
        identifier_WikiData, or uri (ScienceBase Directory)
        :return: GraphQL result (JSON array of objects/list of dictionaries) containing information about coauthors,
        including their identifiers and an indication of whether or not they are USGS employees
        '''
        q_co_authors = '''
          {
            identified_pw_authors(
                where: {
                    _and: {
                        uri: {
                            _in: %s
                        },
                        %s: {
                            _neq: "%s"
                        }
                    }
                }
            ) 
            {
              contributorId
              email
              family
              given
              identifier_ORCID
              sb_uri
              usgs
              publicationYear
            }
          }
        ''' % (str(pub_list).replace("'", '"'), parameter, identifier)
        try:
            query_response = self.execute_query(q_co_authors)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            co_author_list = query_response["data"]["identified_pw_authors"]

            co_authors_with_count = list()
            for contributorId, count in dict(Counter(i["contributorId"] for i in co_author_list)).items():
                author_records = [i for i in co_author_list if i["contributorId"] == contributorId]
                pub_years = [int(i["publicationYear"]) for i in author_records]
                author_record = author_records[0]
                author_record["count"] = count
                author_record["start_year"] = min(pub_years)
                author_record["end_year"] = max(pub_years)
                del author_record["publicationYear"]
                co_authors_with_count.append(author_record)

            return co_authors_with_count

    def lookup_authoring_affiliations(self, pub_list):
        '''
        Looks up the set of author affiliations for a given set of publications.
        :param pub_list: List of publication URL URIs
        :return: List of organization names, the count of occurrences as author affiliations, start and end years of
        publications, along with flags on active and USGS
        '''
        q_affiliations = '''
          {
            pw_affiliations(where: {uri: {_in: %s}}) {
              text
              active
              usgs
              publicationYear
            }
          }
        ''' % (str(pub_list).replace("'", '"'))
        try:
            query_response = self.execute_query(q_affiliations)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            affiliation_list = query_response["data"]["pw_affiliations"]

            affiliations_with_count = list()
            for affiliation, count in dict(Counter(i["text"] for i in affiliation_list)).items():
                affiliation_records = [i for i in affiliation_list if i["text"] == affiliation]
                affiliation_record = {
                    "organization_name": affiliation_records[0]["text"],
                    "organization_active": affiliation_records[0]["active"],
                    "usgs": affiliation_records[0]["usgs"],
                    "count": count,
                    "start_year": min([int(i["publicationYear"]) for i in affiliation_records]),
                    "end_year": max([int(i["publicationYear"]) for i in affiliation_records])
                }
                affiliations_with_count.append(affiliation_record)

            return affiliations_with_count

    def lookup_pub_cost_centers(self, pub_list):
        '''
        Looks up the set of cost centers associated with a list of publications.
        :param pub_list: List of publication URL URIs
        :return: List of USGS Cost Centers, the count of occurrences as in publications, start and end years of
        publications
        '''
        q_cost_centers = '''
          {
            pw_cost_centers(where: {uri: {_in: %s}}) {
              text
              publicationYear
            }
          }
        ''' % (str(pub_list).replace("'", '"'))
        try:
            query_response = self.execute_query(q_cost_centers)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            cost_centers = query_response["data"]["pw_cost_centers"]

            cost_centers_with_count = list()
            for cost_center, count in dict(Counter(i["text"] for i in cost_centers)).items():
                cost_center_records = [i for i in cost_centers if i["text"] == cost_center]
                cost_center_record = {
                    "cost_center_name": cost_center_records[0]["text"],
                    "count": count,
                    "start_year": min([int(i["publicationYear"]) for i in cost_center_records]),
                    "end_year": max([int(i["publicationYear"]) for i in cost_center_records])
                }
                cost_centers_with_count.append(cost_center_record)

            return cost_centers_with_count

    def lookup_pub_entities(self, pub_list):
        '''
        Looks up available pre-cached named entities from a previously run NER process on publication titles and
        abstracts.
        :param pub_list: List of publication URL URIs
        :return: List of dictionaries/objects with name, type, and count on NER entities identified
        '''
        q_entities = '''
          {
            ner_pub_entities(where: {uri: {_in: %s}}) {
              entity_type
              entity_name
              entity_count
            }
          }
        ''' % (str(pub_list).replace("'", '"'))
        try:
            query_response = self.execute_query(q_entities)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            return query_response["data"]["ner_pub_entities"]

    def lookup_wikidata_entity(self, qid):
        '''
        Looks up a cached WikiData entity record based on QID identifier
        :param qid: QID identifier for WikiData entity
        :return: Returns basic information on an entity as simple, properties/values
        '''
        q_wd_entity = '''
            {
              wikidata_entities(where: {id: {_eq: "%s"}}) {
                id
                label_en
                modified
                title
                type
                description_en
                aliases_en
              }
            }
        ''' % (qid)
        try:
            query_response = self.execute_query(q_wd_entity)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            if len(query_response["data"]["wikidata_entities"]) > 1:
                return "Query returned more than one record, and only one record was expected."

            return query_response["data"]["wikidata_entities"][0]

    def lookup_wikidata_claims(self, qid):
        '''
        Looks up the set of statements/claims made about a given WikiData entity
        :param qid: QID identifier for WikiData entity
        :return: Returns assembled data for WikiData claims that includes both string and date values along with
        information from links to other WikiData entities assembled in the iSAID cache
        '''
        q_wd_claims = '''
            {
              identified_wikidata_claims(where: {entity_id: {_eq: "%s"}}) {
                label_en
                description_en
                property_id
                property_value
                ref_entity_description
                ref_entity_label
                entity_id
                datatype
              }
            }
        ''' % (qid)
        try:
            query_response = self.execute_query(q_wd_claims)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            return query_response["data"]["identified_wikidata_claims"]

    def assemble_person_record(self, identifier=None, parameter="email", person_doc=None):
        '''
        Assembles a full logical document for a given person with all available information in the iSAID cache
        :param identifier: Identifier value to uniquely identify individual person
        :param parameter: Variable containing the identifier; should be one of email, identifier_ORCID,
        :param person_doc: A person document from the ScienceBase Directory cache may already be provided through
        another process and can be used for assembly, bypassing the need to lookup a person from an identifier
        identifier_WikiData, or uri (ScienceBase Directory)
        :return: JSON object/dictionary containing logical sections of information for a given person
        '''
        if person_doc is not None:
            person_info = person_doc
        else:
            person_info = self.lookup_person(identifier, parameter=parameter)

        if not isinstance(person_info, dict):
            return None

        person_record = {
            "ScienceBase Directory": person_info
        }

        expertise_terms = self.lookup_expertise(person_info["email"])
        if expertise_terms is not None:
            person_record["Expertise Terms"] = expertise_terms

        pubs_list = self.lookup_pubs(person_info["email"], parameter=parameter)
        if pubs_list is not None:
            person_record["Publications"] = pubs_list
            pubs_uri_list = [i["uri"] for i in pubs_list]

            associated_cost_centers = self.lookup_pub_cost_centers(pubs_uri_list)
            if associated_cost_centers is not None:
                person_record["Associated Cost Centers"] = associated_cost_centers

            associated_co_authors = self.lookup_co_authors(
                pubs_uri_list,
                person_info["email"],
                parameter=parameter
            )
            if associated_co_authors is not None:
                person_record["Associated Coauthors"] = associated_co_authors

            authoring_affiliations = self.lookup_authoring_affiliations(pubs_uri_list)
            if authoring_affiliations is not None:
                person_record["Authoring Affiliations"] = authoring_affiliations

            entities_from_pubs = self.lookup_pub_entities(pubs_uri_list)
            if entities_from_pubs is not None:
                person_record["Named Entities in Publications"] = entities_from_pubs

        if person_info["identifier_WikiData"] is not None:
            wikidata_entity = self.lookup_wikidata_entity(person_info["identifier_WikiData"])
            if wikidata_entity is not None:
                person_record["WikiData Entity"] = wikidata_entity
            wikidata_claims = self.lookup_wikidata_claims(person_info["identifier_WikiData"])
            if wikidata_claims is not None:
                person_record["WikiData Statements"] = wikidata_claims

        return person_record

    def get_people(self, email_list=None):
        '''
        Queries the ScienceBase Directory cache in the iSAID database for a set of records
        :param email_list: List of email addresses to use in constraining search
        :return: List of dictionaries containing person records.
        '''
        where_clause = ""
        if email_list is not None:
            where_clause = "(where: {email: {_in: %s}})" % (str(email_list).replace("'", '"'))

        q_people = '''
            {
              sb_usgs_employees %s {
                url
                uri
                state
                professionalQualifier
                personalTitle
                organization_uri
                organization_name
                orcId
                note
                middleName
                lastName
                jobTitle
                index
                identifier_WikiData
                identifier_ORCID
                generationalQualifier
                firstName
                email
                displayName
                description
                date_cached
                city
                cellPhone
              }
            }
        ''' % (where_clause)
        try:
            query_response = self.execute_query(q_people)
        except ValueError as e:
            return e

        if "errors" in query_response.keys():
            return query_response
        else:
            return query_response["data"]["sb_usgs_employees"]
