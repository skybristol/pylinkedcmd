import requests
from sciencebasepy import SbSession
import validators
from fuzzywuzzy import fuzz
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import json
from getpass import getpass
import pydash
import copy

class Sciencebase:
    def __init__(self):
        self.description = "Set of functions for working with the ScienceBase Directory"
        self.sb_directory_people_api = "https://www.sciencebase.gov/directory/people"
        self.fix_identifiers = ["ORCID", "WikiData"]
        self.wd = Wikidata()

    def lookup_sb_person_by_email(self, email):
        '''
        Searches the ScienceBase Directory for a person by their email address. Returns None if either no record or
        more than one record is found because we cannot act on these in either case.

        :param email: Must be a valid email string
        :return: Either None if not found or the correctly formatted full person document (dict)
        '''
        if not validators.email(email):
            raise ValueError("You must supply a valid email address")

        r = requests.get(
            f"{self.sb_directory_people_api}?format=json&dataset=all&email={email}"
        ).json()

        if len(r["people"]) != 1:
            return None
        else:
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

    def get_all_orcid(self):
        '''
        Uses the ScienceBase Directory search API to retrieve all person records with ORCID values. The ORCIDs were
        put into their own top-level property for some reason instead of using the identifiers object. We can use this
        function to grab everything and then run all documents through an update process. The top-level attribute is
        where ORCID values are synchronized in from the DOI Active Directory, however, so we will need to continue to
        maintain that connection.

        :return: List of dictionaries containing simplified attributes for person documents having an ORCID
        '''
        orcid_person_list = list()
        next_link = f"{self.sb_directory_people_api}?format=json&dataset=all&lq=_exists_:orcId&max=1000"

        while next_link is not None:
            data = requests.get(next_link).json()

            if len(data["people"]) > 0:
                for person in data["people"]:
                    this_person = {
                                "api": person["link"]["href"],
                                "displayName": person["displayName"],
                                "email": person["email"],
                                "orcid": person["orcId"]
                            }
                    if "identifiers" in person.keys():
                        this_person["identifiers"] = person["identifiers"]

                    orcid_person_list.extend(this_person)

            if "nextlink" in data.keys():
                next_link = data["nextlink"]["url"]
            else:
                next_link = None

        return orcid_person_list


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
            raise ValueError("The identifier provided resulted in no results from WikiData")

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
            raise ValueError("The identifier provided resulted in no results from WikiData")

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
