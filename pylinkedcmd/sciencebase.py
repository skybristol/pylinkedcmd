from sciencebasepy import SbSession
import requests
import validators
from datetime import datetime
import dateutil
from copy import copy
import unidecode
from getpass import getpass
import re
import pandas as pd


class Directory:
    def __init__(self, authenticated=False):
        self.authenticated = authenticated
        self.sb_root_url = "https://www.sciencebase.gov/directory/people?format=json"
        self.sb_org_search_url = "https://www.sciencebase.gov/directory/organizations?format=json"
        self.sb_org_root = "https://www.sciencebase.gov/directory/organization/"
        self.sb_person_root = "https://www.sciencebase.gov/directory/person/"
        self.orcid_pattern = r"\d{4}-\d{4}-\d{4}-\w{4}"
        self.sb = SbSession()

        if authenticated:
            self.sb.login(input("User Name: "), getpass("Password: "))

    def lookup_person(
        self, 
        criteria, 
        unique=True, 
        verifier_operator=None, 
        verifier_criteria=None, 
        attempt_last_name=True
    ):
        if validators.email(criteria):
            q_operator = "email"
        elif re.search(self.orcid_pattern, criteria):
            q_operator = "q"
            verifier_operator = "orcId"
            verifier_criteria = criteria
        else:
            q_operator = "q"
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

    def all_people(self):

        people_listing = list()

        next_url = f"{self.sb_root_url}&max=1000"
        while next_url is not None:
            sb_results = self.sb._session.get(next_url).json()
            if "people" in sb_results and len(sb_results["people"]) > 0:
                people_listing.extend(sb_results["people"])
            if "nextlink" in sb_results:
                next_url = sb_results["nextlink"]["url"]
            else:
                next_url = None

        filtered_people = [
            i for i in people_listing
            if i["distinguishedName"] is not None
            and i["email"] is not None
            and "OU=Shared Mailboxes" not in i["distinguishedName"]
            and "OU=Service Accounts" not in i["distinguishedName"]
            and "usgs.gov" in i["email"]
        ]

        return filtered_people

    def all_orgs(self):
        org_listing = list()

        next_url = f"{self.sb_org_search_url}&max=1000"
        while next_url is not None:
            sb_results = self.sb._session.get(next_url).json()
            if "organizations" in sb_results and len(sb_results["organizations"]) > 0:
                org_listing.extend(sb_results["organizations"])
            if "nextlink" in sb_results:
                next_url = sb_results["nextlink"]["url"]
            else:
                next_url = None

        return org_listing  

    def add_last_update(self, sbid):
        if not self.authenticated:
            return

        self.sb._session.headers.update({'Accept': 'text/html'})
        r = self.sb._session.get(sbid)
        tables = pd.read_html(r.text)
        return {
            "sbid": sbid,
            "last_updated": dateutil.parser.parse(tables[0].loc[tables[0][0] == "Last Updated"][1].values[0], tzinfos=timezone_info),
            "last_updated_by": tables[0].loc[tables[0][0] == "Last Updated By"][1].values[0]
        }


timezone_info = {
    "A": 1 * 3600,
    "ACDT": 10.5 * 3600,
    "ACST": 9.5 * 3600,
    "ACT": -5 * 3600,
    "ACWST": 8.75 * 3600,
    "ADT": 4 * 3600,
    "AEDT": 11 * 3600,
    "AEST": 10 * 3600,
    "AET": 10 * 3600,
    "AFT": 4.5 * 3600,
    "AKDT": -8 * 3600,
    "AKST": -9 * 3600,
    "ALMT": 6 * 3600,
    "AMST": -3 * 3600,
    "AMT": -4 * 3600,
    "ANAST": 12 * 3600,
    "ANAT": 12 * 3600,
    "AQTT": 5 * 3600,
    "ART": -3 * 3600,
    "AST": 3 * 3600,
    "AT": -4 * 3600,
    "AWDT": 9 * 3600,
    "AWST": 8 * 3600,
    "AZOST": 0 * 3600,
    "AZOT": -1 * 3600,
    "AZST": 5 * 3600,
    "AZT": 4 * 3600,
    "AoE": -12 * 3600,
    "B": 2 * 3600,
    "BNT": 8 * 3600,
    "BOT": -4 * 3600,
    "BRST": -2 * 3600,
    "BRT": -3 * 3600,
    "BST": 6 * 3600,
    "BTT": 6 * 3600,
    "C": 3 * 3600,
    "CAST": 8 * 3600,
    "CAT": 2 * 3600,
    "CCT": 6.5 * 3600,
    "CDT": -5 * 3600,
    "CEST": 2 * 3600,
    "CET": 1 * 3600,
    "CHADT": 13.75 * 3600,
    "CHAST": 12.75 * 3600,
    "CHOST": 9 * 3600,
    "CHOT": 8 * 3600,
    "CHUT": 10 * 3600,
    "CIDST": -4 * 3600,
    "CIST": -5 * 3600,
    "CKT": -10 * 3600,
    "CLST": -3 * 3600,
    "CLT": -4 * 3600,
    "COT": -5 * 3600,
    "CST": -6 * 3600,
    "CT": -6 * 3600,
    "CVT": -1 * 3600,
    "CXT": 7 * 3600,
    "ChST": 10 * 3600,
    "D": 4 * 3600,
    "DAVT": 7 * 3600,
    "DDUT": 10 * 3600,
    "E": 5 * 3600,
    "EASST": -5 * 3600,
    "EAST": -6 * 3600,
    "EAT": 3 * 3600,
    "ECT": -5 * 3600,
    "EDT": -4 * 3600,
    "EEST": 3 * 3600,
    "EET": 2 * 3600,
    "EGST": 0 * 3600,
    "EGT": -1 * 3600,
    "EST": -5 * 3600,
    "ET": -5 * 3600,
    "F": 6 * 3600,
    "FET": 3 * 3600,
    "FJST": 13 * 3600,
    "FJT": 12 * 3600,
    "FKST": -3 * 3600,
    "FKT": -4 * 3600,
    "FNT": -2 * 3600,
    "G": 7 * 3600,
    "GALT": -6 * 3600,
    "GAMT": -9 * 3600,
    "GET": 4 * 3600,
    "GFT": -3 * 3600,
    "GILT": 12 * 3600,
    "GMT": 0 * 3600,
    "GST": 4 * 3600,
    "GYT": -4 * 3600,
    "H": 8 * 3600,
    "HDT": -9 * 3600,
    "HKT": 8 * 3600,
    "HOVST": 8 * 3600,
    "HOVT": 7 * 3600,
    "HST": -10 * 3600,
    "I": 9 * 3600,
    "ICT": 7 * 3600,
    "IDT": 3 * 3600,
    "IOT": 6 * 3600,
    "IRDT": 4.5 * 3600,
    "IRKST": 9 * 3600,
    "IRKT": 8 * 3600,
    "IRST": 3.5 * 3600,
    "IST": 5.5 * 3600,
    "JST": 9 * 3600,
    "K": 10 * 3600,
    "KGT": 6 * 3600,
    "KOST": 11 * 3600,
    "KRAST": 8 * 3600,
    "KRAT": 7 * 3600,
    "KST": 9 * 3600,
    "KUYT": 4 * 3600,
    "L": 11 * 3600,
    "LHDT": 11 * 3600,
    "LHST": 10.5 * 3600,
    "LINT": 14 * 3600,
    "M": 12 * 3600,
    "MAGST": 12 * 3600,
    "MAGT": 11 * 3600,
    "MART": 9.5 * 3600,
    "MAWT": 5 * 3600,
    "MDT": -6 * 3600,
    "MHT": 12 * 3600,
    "MMT": 6.5 * 3600,
    "MSD": 4 * 3600,
    "MSK": 3 * 3600,
    "MST": -7 * 3600,
    "MT": -7 * 3600,
    "MUT": 4 * 3600,
    "MVT": 5 * 3600,
    "MYT": 8 * 3600,
    "N": -1 * 3600,
    "NCT": 11 * 3600,
    "NDT": 2.5 * 3600,
    "NFT": 11 * 3600,
    "NOVST": 7 * 3600,
    "NOVT": 7 * 3600,
    "NPT": 5.5 * 3600,
    "NRT": 12 * 3600,
    "NST": 3.5 * 3600,
    "NUT": -11 * 3600,
    "NZDT": 13 * 3600,
    "NZST": 12 * 3600,
    "O": -2 * 3600,
    "OMSST": 7 * 3600,
    "OMST": 6 * 3600,
    "ORAT": 5 * 3600,
    "P": -3 * 3600,
    "PDT": -7 * 3600,
    "PET": -5 * 3600,
    "PETST": 12 * 3600,
    "PETT": 12 * 3600,
    "PGT": 10 * 3600,
    "PHOT": 13 * 3600,
    "PHT": 8 * 3600,
    "PKT": 5 * 3600,
    "PMDT": -2 * 3600,
    "PMST": -3 * 3600,
    "PONT": 11 * 3600,
    "PST": -8 * 3600,
    "PT": -8 * 3600,
    "PWT": 9 * 3600,
    "PYST": -3 * 3600,
    "PYT": -4 * 3600,
    "Q": -4 * 3600,
    "QYZT": 6 * 3600,
    "R": -5 * 3600,
    "RET": 4 * 3600,
    "ROTT": -3 * 3600,
    "S": -6 * 3600,
    "SAKT": 11 * 3600,
    "SAMT": 4 * 3600,
    "SAST": 2 * 3600,
    "SBT": 11 * 3600,
    "SCT": 4 * 3600,
    "SGT": 8 * 3600,
    "SRET": 11 * 3600,
    "SRT": -3 * 3600,
    "SST": -11 * 3600,
    "SYOT": 3 * 3600,
    "T": -7 * 3600,
    "TAHT": -10 * 3600,
    "TFT": 5 * 3600,
    "TJT": 5 * 3600,
    "TKT": 13 * 3600,
    "TLT": 9 * 3600,
    "TMT": 5 * 3600,
    "TOST": 14 * 3600,
    "TOT": 13 * 3600,
    "TRT": 3 * 3600,
    "TVT": 12 * 3600,
    "U": -8 * 3600,
    "ULAST": 9 * 3600,
    "ULAT": 8 * 3600,
    "UTC": 0 * 3600,
    "UYST": -2 * 3600,
    "UYT": -3 * 3600,
    "UZT": 5 * 3600,
    "V": -9 * 3600,
    "VET": -4 * 3600,
    "VLAST": 11 * 3600,
    "VLAT": 10 * 3600,
    "VOST": 6 * 3600,
    "VUT": 11 * 3600,
    "W": -10 * 3600,
    "WAKT": 12 * 3600,
    "WARST": -3 * 3600,
    "WAST": 2 * 3600,
    "WAT": 1 * 3600,
    "WEST": 1 * 3600,
    "WET": 0 * 3600,
    "WFT": 12 * 3600,
    "WGST": -2 * 3600,
    "WGT": -3 * 3600,
    "WIB": 7 * 3600,
    "WIT": 9 * 3600,
    "WITA": 8 * 3600,
    "WST": 14 * 3600,
    "WT": 0 * 3600,
    "X": -11 * 3600,
    "Y": -12 * 3600,
    "YAKST": 10 * 3600,
    "YAKT": 9 * 3600,
    "YAPT": 10 * 3600,
    "YEKST": 6 * 3600,
    "YEKT": 5 * 3600,
    "Z": 0 * 3600,
}