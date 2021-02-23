import requests
from datetime import datetime
import validators
import re
from bs4 import BeautifulSoup
from copy import copy
from . import utilities


class UsgsWeb:
    def __init__(self):
        self.usgs_pro_page_listing = "https://www.usgs.gov/connect/staff-profiles"
        self.usgs_science_center_listing = "https://www.usgs.gov/usgs-science-centers"
        self.expertise_link_pattern = re.compile(r"^\/science-explorer-results\?*")
        self.profile_link_pattern = re.compile(r"^\/staff-profiles\/*")
        self.orcid_link_pattern = re.compile(r"^https:\/\/orcid.org\/*")
        self.mailto_link_pattern = re.compile(r"^mailto:")
        self.tel_link_pattern = re.compile(r"^tel:")
        self.org_link_pattern = re.compile(r"www.usgs.gov")
        self.usgs_web_root = "https://www.usgs.gov"

    def get_staff_inventory_pages(self, link=None, title_="Go to last page"):
        '''
        Unfortunately, the only way to get the entire staff inventory as presented on the USGS web that I've found is to
        iterate over every page in the closed web system and scrape listings. To figure out what pages are contained in
        the listing without filters, we need to consult the first page and get a page number for the last listing from
        a particular link. This function handles that process and gives us back every URL we need to hit.
        :param title_: title of the link pointing to the last page of the inventory
        :type title_: str
        :return: list of URLs to every page comprising the entire inventory of USGS staff
        '''
        if link is None:
            link = self.usgs_pro_page_listing

        r = requests.get(link)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.content, 'html.parser')

        last_page_link = soup.findAll("a", title=title_)
        if len(last_page_link) == 0:
            return [link]

        last_page_num = last_page_link[0]["href"].split("=")[-1]

        inventory_url_list = [
            f"{link}?page={n}" for n in range(0, int(last_page_num)+1)
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
            "_date_cached": str(datetime.utcnow().isoformat()),
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
            return {"url": page_url, "error": f"Status-code: {r.status_code}"}

        soup = BeautifulSoup(r.content, 'html.parser')

        profile_page_data = {
            "profile": page_url,
            "_date_cached": datetime.utcnow().isoformat(),
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
            profile_page_data["scraped_body_html"] = profile_body_content.decompose()
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
                profile_page_data["organization_name"] = organization_link_container.text.strip()

        profile_image = soup.find("img", class_='staff-profile-image')
        if profile_image is not None:
            profile_page_data["profile_image_url"] = profile_image["src"]

        orcid_link = soup.find("a", href=self.orcid_link_pattern)
        if orcid_link is not None:
            check_id = utilities.actionable_id(orcid_link.text)
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

        return profile_page_data

    def science_center_inventory(self):
        r_sc_listing = requests.get(self.usgs_science_center_listing)

        soup_sc_listing = BeautifulSoup(r_sc_listing.text, 'html.parser')

        table_sc_listing = soup_sc_listing.find('table')

        table_links = list()
        for link in table_sc_listing.findAll("a"):
            if "http" in link["href"].lower():
                link_url = link["href"]
            else:
                link_url = f'{self.usgs_web_root}{link["href"]}'
            table_links.append((link.text, link_url))

        science_centers = list()
        for row_index, row in enumerate(table_sc_listing.findAll("tr")):
            if row_index > 0:
                science_center_record = {
                    "_date_cached": str(datetime.utcnow().isoformat()),
                }
                
                for index,col in enumerate(row.findAll("td")):
                    if index == 0:
                        science_center_record["name"] = col.text.strip()
                        for link in col.findAll("a"):
                            science_center_record["url"] = next((i[1] for i in table_links if i[0] == col.text), None)
                    elif index == 1:
                        if "(" in col.text:
                            science_center_record["center_director_qualifier"] = col.text.split("(")[-1].replace(")", "").strip().lower()
                            science_center_record["center_director"] = col.text.split("(")[0].strip()
                        else:
                            science_center_record["center_director"] = col.text.strip()
                        science_center_record["center_director_link"] = next((i[1] for i in table_links if i[0] == science_center_record["center_director"]), None)
                    elif index == 2:
                        if col.text != "ALL":
                            science_center_record["regions"] = [i.strip() for i in col.text.split(",")]
                    elif index == 3:
                        science_center_record["state_or_territory"] = [i.strip() for i in col.text.split(",")]

                employee_directory_url = f'{science_center_record["url"]}/employee-directory'
                r_employee_directory = requests.get(employee_directory_url)
                if r_employee_directory.status_code == 200:
                    science_center_record["url_employee_directory"] = employee_directory_url

                science_center_locations_url = f'{science_center_record["url"]}/locations'
                r_center_locations = requests.get(science_center_locations_url)
                if r_center_locations.status_code == 200:
                    science_center_record["url_locations"] = science_center_locations_url

                science_center_science_url = f'{science_center_record["url"]}/science'
                r_center_science_page = requests.get(science_center_science_url)
                if r_center_science_page.status_code == 200:
                    science_center_record["url_science"] = science_center_science_url

                science_centers.append(science_center_record)

        return science_centers

    def employee_directory(self, sc_inventory_record):
        if "url_employee_directory" not in sc_inventory_record:
            return
        
        directory_urls = self.get_staff_inventory_pages(link=sc_inventory_record["url_employee_directory"])

        if len(directory_urls) == 0:
            return

        employee_listing = list()
        for url in directory_urls:
            r = requests.get(url)
            soup = BeautifulSoup(r.text, 'html.parser')
            table = soup.findAll("table")[0]
            tbody = table.findAll("tbody")[0]

            person_records = list()
            for row in tbody.findAll("tr"):
                person_record = {
                    "_date_cached": str(datetime.utcnow().isoformat()),
                    "science_center_name": sc_inventory_record["name"],
                    "science_center_url": sc_inventory_record["url"],
                    "science_center_employee_directory": sc_inventory_record["url_employee_directory"],
                }
                if "fbms_code" in sc_inventory_record:
                    person_record["fbms_code"] = sc_inventory_record["fbms_code"]

                for index, col in enumerate(row.findAll("td")):
                    if index == 0:
                        person_record["title"] = col.text.strip()
                    elif index == 1:
                        person_record["name"] = col.text.strip()
                        link = col.find("a", href=True)
                        if link is not None:
                            if "http" in link["href"].lower():
                                person_record["usgs_web_url"] = link["href"]
                            else:
                                person_record["usgs_web_url"] = f'{self.usgs_web_root}{link["href"]}'
                    elif index == 2:
                        person_record["email"] = col.text.strip()
                    elif index == 3:
                        person_record["telephone"] = col.text.strip()
                person_records.append(person_record)

            employee_listing.extend(person_records)

        return employee_listing

    def sc_locations(self, sc_inventory_record):
        if "url_locations" not in sc_inventory_record:
            return

        r = requests.get(sc_inventory_record["url_locations"])

        if r.status_code != 200:
            return

        soup = BeautifulSoup(r.text, 'html.parser')

        locations = list()
        for loc in soup.findAll("div", {"class": "col-sm-7"}):
            location = {
                "_date_cached": str(datetime.utcnow().isoformat()),
                "science_center_name": sc_inventory_record["name"],
                "science_center_url": sc_inventory_record["url"],
                "science_center_locations": sc_inventory_record["url_locations"],
            }

            name_container = loc.find("h3", {'class': 'h4'})
            name_link = name_container.find("a")
            thoroughfare_container = loc.find('div', {'class': 'thoroughfare'})
            locality_container = loc.find('span', {'class': 'locality'})
            state_container = loc.find('span', {'class': 'state'})
            postal_code_container = loc.find('span', {'class': 'postal-code'})
            country_container = loc.find('span', {'class': 'country'})

            if name_container is not None:
                location["location_name"] = name_container.text.strip()

                if name_link is not None:
                    location["location_url"] = name_link["href"]

                if thoroughfare_container is not None:
                    location["location_address"] = thoroughfare_container.text.strip()
            
                if locality_container is not None:
                    location["location_locality"] = locality_container.text.strip()
            
                if state_container is not None:
                    location["location_state"] = state_container.text.strip()

                if postal_code_container is not None:
                    location["location_postal_code"] = postal_code_container.text.strip()

                if country_container is not None:
                    location["location_countryt"] = country_container.text.strip()

                locations.append(location)
            
        if not locations:
            return

        return locations

    def sc_topics(self, sc_inventory_record):
        if "url_science" not in sc_inventory_record:
            return

        r = requests.get(sc_inventory_record["url_science"])

        if r.status_code != 200:
            return

        soup = BeautifulSoup(r.text, 'html.parser')

        subjects_addressed = list()
        subject = {
            "_date_cached": str(datetime.utcnow().isoformat()),
            "science_center_name": sc_inventory_record["name"],
            "science_center_url": sc_inventory_record["url"],
            "science_center_topics": sc_inventory_record["url_science"],
        }

        theme_container = soup.find("div", {'class': 'view-content'})
        if theme_container is not None:
            for theme in theme_container.findAll("h3", {'class': 'h4'}):
                theme_item = copy(subject)
                theme_item["subject_type"] = "science theme"
                theme_item["subject"] = theme.text.strip()

                theme_link = theme.find("a")
                if theme_link is not None:
                    theme_item["subject_link"] = f'{self.usgs_web_root}{theme_link["href"]}'

                subjects_addressed.append(theme_item)

        science_topic_container = soup.find("div", {'id': 'science-pane-list'})
        if science_topic_container is not None:
            for topic_link in science_topic_container.findAll("a"):
                topic_item = copy(subject)
                topic_item["subject_type"] = "science topic"
                topic_item["subject"] = topic_link.text.strip()
                topic_item["subject_link"] = f'{self.usgs_web_root}{topic_link["href"]}'

                subjects_addressed.append(topic_item)

        if not subjects_addressed:
            return

        return subjects_addressed