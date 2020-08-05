import requests
from sciencebasepy import SbSession
import validators
from fuzzywuzzy import fuzz
import sys
from SPARQLWrapper import SPARQLWrapper, JSON
import json
from getpass import getpass

class Sciencebase:
    def __init__(self):
        self.description = "Set of functions for working with the ScienceBase Directory"
        self.sb_directory_people_api = "https://www.sciencebase.gov/directory/people"
        self.fix_identifiers = ["ORCID", "WikiData"]
        self.wd = Wikidata()

    def get_identified_sb_person(self, email):
        '''
        Function does some specialized stuff with ScienceBase Directory person documents to package identifiers into
        the identifiers object in a way that supports linked data operations. The purpose here is to generate a
        revised person document to commit to the ScienceBase Directory that supports further operations, based on the
        available identifiers, such as dynamically assembling a research record (publications, datasets, etc.).

        :param email: Generally this is an email address which is mostly unique in the ScienceBase Directory and can be
        used to pull out a specific person record without needing to know the internal id
        :return: Python set containing a revised person record with new identifiers object and a boolean value indicating
        whether or not the given person record was modified with the new identifiers construct
        '''
        r = requests.get(
            f"{self.sb_directory_people_api}?format=json&dataset=all&email={email}"
        ).json()

        if len(r["people"]) == 0:
            return email
        elif len(r["people"]) > 1:
            return email
        else:
            # We have to get the person record with a separate process because it's different than what comes
            # back in the search result
            person_record = requests.get(f'{r["people"][0]["link"]["href"]}?format=json').json()

            if "identifiers" in person_record.keys():
                person_identifiers = person_record["identifiers"]
            else:
                person_identifiers = list()

            # Right now having an ORCID is the trigger that we can link out to other stuff, so this only happens if
            # we have an ORCID on board the person document
            if "orcId" in person_record.keys():
                name_list = [person_record["displayName"]]
                if "aliases" in person_record.keys():
                    name_list.extend([n["name"] for n in person_record["aliases"]])

                person_identifiers = self.package_orcid_wikidata_ids(
                    person_record["orcId"],
                    name_list=name_list,
                    existing_ids=person_identifiers
                )

            if len(person_identifiers) > 0:
                person_record["identifiers"] = person_identifiers
                person_package = (person_record, True)
            else:
                person_package = (person_record, False)

            return person_package

    def update_sb_person_identifiers(self, emails):
        '''
        Works through a list of emails and executes update operations on them to update or insert identifiers into the
        person documents in the ScienceBase Directory in order to facilitate linked data operations. The function uses
        sciencebasepy to establish an authenticated session with requests in order to issue updates to the ScienceBase
        Directory API. This comes with a username and password prompt.

        The function will raise an error if there are invalid email address strings instead of continuing. It will also
        raise an error if any of the emails result in person records that can't be retrieved with
        get_identified_sb_person().

        :param emails: List of email addresses; puts a single string into a list if needed
        :return: Python dictionary containing lists of person documents that were updated and/or not updated (because
        they didn't have anything able to be updated)
        '''
        if not isinstance(emails, list):
            emails = [emails]

        email_check = [(email, validators.email(email)) for email in emails]
        valid_emails = [em[0] for em in email_check if em[1]]
        invalid_emails = [em[0] for em in email_check if not em[1]]

        person_records = [self.get_identified_sb_person(email) for email in valid_emails]

        processable_person_records = [p for p in person_records if isinstance(p, tuple)]
        non_processable_person_records = [p for p in person_records if isinstance(p, str)]

        update_person_records = [i[0] for i in processable_person_records if i[1]]
        no_update_person_records = [i[0] for i in processable_person_records if not i[1]]

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
            "invalidEmails": invalid_emails,
            "nonProcessableEmails": non_processable_person_records
        }

    def package_orcid_wikidata_ids(self, orcid, name_list=list(), existing_ids=list()):
        '''
        Packages an orcid in the manner ScienceBase Directory uses and then attempts to lookup a corresponding WikiData
        ID and packages that if found. This function is the key part of the process used in building out the identifiers
        in a ScienceBase person document.

        :param orcid: ORCID from the ScienceBase Person document
        :param name_list: List of names (including aliases if available) to help make sure we get the right WikiData ID
        :param existing_ids: Any existing identifiers from the ScienceBase Person document
        :return: List of dictionaries in structure used by ScienceBase with type and key properties
        '''
        if len(existing_ids) > 0:
            # Get rid of the identifiers we are going to refactor, keeping anything else
            existing_ids = [i for i in existing_ids if i["type"] not in self.fix_identifiers]

        existing_ids.append(
            {
                "type": "ORCID",
                "key": orcid
            }
        )

        wikidata_id = self.wd.lookup_wikidata_by_orcid(orcid, name_list)

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
                orcid_person_list.extend(
                    [
                        {
                            "api": i["link"]["href"],
                            "displayName": i["displayName"],
                            "email": i["email"],
                            "orcid": i["orcId"]
                        }
                        for i in data["people"]
                    ]
                )

            if "nextlink" in data.keys():
                next_link = data["nextlink"]["url"]
            else:
                next_link = None

        return orcid_person_list


class Wikidata:
    def __init__(self):
        self.description = "Set of functions for interacting with the WikiData API"
        self.wikidata_endpoint = "https://query.wikidata.org/sparql"
        self.user_agent = f"pylinkedcmd/{sys.version_info[0]}.{sys.version_info[1]}"

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

        sparql = SPARQLWrapper(self.wikidata_endpoint, agent=self.user_agent)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        result_set = sparql.query().convert()

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
