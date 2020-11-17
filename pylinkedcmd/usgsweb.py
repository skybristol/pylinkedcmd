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

from . import pylinkedcmd


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
            staff_listing = self.process_staff_section(section)
            if "profile" in staff_listing.keys() and staff_listing["profile"] is not None:
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
                person_record["name"] = name_container.text.replace("\t", "").strip()

        if org_link is not None:
            person_record["organization_name"] = org_link.text.replace("\t", "").strip()
            person_record["organization_link"] = org_link["href"].strip()

        if email_link is not None:
            person_record["email"] = email_link.text.replace("\t", "").strip().lower()

        if tel_link is not None:
            person_record["telephone"] = tel_link.text.replace("\t", "").strip()

        bolded_item = section.find("b")
        if bolded_item is not None:
            person_record["title"] = bolded_item.text.replace("\t", "").strip()

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
            "expertise": list()
        }

        expertise_section = soup.find("section", class_="staff-expertise")
        if expertise_section is not None:
            profile_page_data["expertise"] = [
                t.text.strip() for t in expertise_section.findAll("a", href=self.expertise_link_pattern)
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
                email_string = email_link.text.lower().strip()
                if validators.email(email_string):
                    profile_page_data["email"] = email_string
                else:
                    profile_page_data["email"] = None

        organization_container = soup.find("h3", class_="staff-profile-subtitle h4")
        if organization_container is not None:
            organization_link_container = organization_container.find("a")
            if organization_link_container is not None:
                profile_page_data["organization_link"] = organization_link_container["href"].strip()
                profile_page_data["organization_name"] = organization_link_container.text.strip().title()

        profile_image = soup.find("img", class_='staff-profile-image')
        if profile_image is not None:
            profile_page_data["profile_image_url"] = profile_image["src"]

        orcid_link = soup.find("a", href=self.orcid_link_pattern)
        if orcid_link is not None:
            check_id = pylinkedcmd.actionable_id(orcid_link.text)
            if check_id is not None and "orcid" in check_id:
                profile_page_data["orcid"] = check_id["orcid"]

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

        expertise_claims = list()

        claim_root = {
            "claim_created": datetime.utcnow().isoformat(),
            "claim_source": "USGS Profile Page",
            "reference": page_url,
            "date_qualifier": datetime.utcnow().isoformat(),
            "subject_instance_of": "Person",
            "subject_identifiers": dict(),
            "property_label": "expertise",
            "object_instance_of": "UnlinkedTerm",
            "object_qualifier": "subject personal assertion"
        }

        if profile_page_data["display_name"] is not None:
            claim_root["subject_label"] = profile_page_data["display_name"]

        if profile_page_data["email"] is not None:
            claim_root["subject_identifiers"]["email"] = profile_page_data["email"]

        if profile_page_data["orcid"] is not None:
            claim_root["subject_identifiers"]["orcid"] = profile_page_data["orcid"]

        if any(k in claim_root["subject_identifiers"].keys() for k in ["subject_email", "subject_orcid"]):
            for expertise_term in profile_page_data["expertise"]:
                expertise_claim = deepcopy(claim_root)
                expertise_claim["object_label"] = expertise_term
                expertise_claims.append(expertise_claim)

        return {
            "summary": profile_page_data,
            "claims": expertise_claims
        }
