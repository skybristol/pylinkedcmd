from datetime import datetime
import validators
from copy import copy
import hashlib
from itertools import groupby
from operator import itemgetter
from . import utilities


def id_claims(claims, include_uid=True, unwind_ids=True):
    # Take care of issue with some missing object labels
    claims = [i for i in claims if len(i["object_label"]) > 0]

    for claim in claims:
        claim["claim_id"] = ":".join([
            claim["claim_source"],
            claim["subject_label"],
            claim["property_label"],
            claim["object_label"]
        ])
        if include_uid:
            claim["claim_uid"] = hashlib.md5(claim["claim_id"].encode('utf-8')).hexdigest()
        
        if unwind_ids:
            if "subject_identifiers" in claim:
                for k,v in claim["subject_identifiers"].items():
                    claim[f"subject_identifier_{k}"] = v
            if "object_identifiers" in claim:
                for k,v in claim["object_identifiers"].items():
                    claim[f"object_identifier_{k}"] = v

    return claims


def claims_from_usgs_profile_inventory(data):
    claims = list()

    claim_stub = {
        "claim_created": str(datetime.utcnow().isoformat()),
        "claim_source": "USGS Profile Inventory",
        "reference": "https://www.usgs.gov/connect/staff-profiles",
        "subject_identifiers": {
            "name": data["name"].strip()
        },
        "subject_instance_of": "Person",
        "subject_label": data["name"].strip()
    }

    if "email" in data and data["email"] is not None and validators.email(data["email"].strip()):
        claim_stub["subject_identifiers"]["email"] = data["email"].strip()

    if "profile" in data and data["profile"] is not None and validators.url(data["profile"].strip()):
        claim_stub["subject_identifiers"]["usgs_web_url"] = data["profile"].strip()

    if "date_cached" in data:
        claim_stub["date_qualifier"] = data["date_cached"]
    else:
        claim_stub["date_qualifier"] = str(datetime.utcnow().isoformat())

    if "title" in data and data["title"] is not None:
        title_claim = copy(claim_stub)
        title_claim["property_label"] = "job title"
        title_claim["object_instance_of"] = "FieldOfWork"
        title_claim["object_label"] = data["title"].strip()
        claims.append(title_claim)

    if "telephone" in data and data["telephone"] is not None:
        telephone_claim = copy(claim_stub)
        telephone_claim["property_label"] = "telephone number"
        telephone_claim["object_instance_of"] = "ContactMethod"
        telephone_claim["object_label"] = data["telephone"].strip()
        claims.append(telephone_claim)

    if "organization_name" in data and data["organization_name"] is not None:
        org_claim = copy(claim_stub)
        org_claim["property_label"] = "employed by"
        org_claim["object_instance_of"] = "Organization"
        org_claim["object_label"] = data["organization_name"].strip()
        if "organization_link" in data and data["organization_link"] is not None and validators.url(data["organization_link"].strip()):
            org_claim["object_identifiers"] = {
                "usgs_web_url": data["organization_link"].strip()
            }
        else:
            org_claim["object_identifiers"] = None
        claims.append(org_claim)

        org_person_claim = {
            "claim_created": claim_stub["claim_created"],
            "claim_source": claim_stub["claim_source"],
            "reference": claim_stub["reference"],
            "property_label": "employs person",
            "date_qualifier": claim_stub["date_qualifier"],
            "object_instance_of": claim_stub["subject_instance_of"],
            "object_label": claim_stub["subject_label"],
            "object_identifiers": claim_stub["subject_identifiers"],
            "subject_instance_of": org_claim["object_instance_of"],
            "subject_label": org_claim["object_label"],
            "subject_identifiers": org_claim["object_identifiers"]
        }
        claims.append(org_person_claim)
    
    return id_claims(claims)


def claims_from_usgs_profile(data):
    claims = list()

    claim_stub = {
        "claim_created": str(datetime.utcnow().isoformat()),
        "claim_source": "USGS Profile Page",
        "reference": data["profile"].strip(),
        "subject_identifiers": {
            "name": data["display_name"].strip(),
            "usgs_web_url": data["profile"].strip()
        },
        "subject_instance_of": "Person",
        "subject_label": data["display_name"].strip()
    }

    if "email" in data and data["email"] is not None and validators.email(data["email"].strip()):
        claim_stub["subject_identifiers"]["email"] = data["email"].strip()

    if "orcid" in data and data["orcid"] is not None:
        check_orcid = utilities.actionable_id(data["orcid"].strip())
        if check_orcid is not None:
            claim_stub["subject_identifiers"]["orcid"] = data["orcid"].strip()

    if "date_cached" in data:
        claim_stub["date_qualifier"] = data["date_cached"]
    else:
        claim_stub["date_qualifier"] = str(datetime.utcnow().isoformat())

    if "organization_name" in data and data["organization_name"] is not None:
        org_claim = copy(claim_stub)
        org_claim["property_label"] = "employed by"
        org_claim["object_instance_of"] = "Organization"
        org_claim["object_label"] = data["organization_name"].strip()
        if "organization_link" in data and data["organization_link"] is not None and validators.url(data["organization_link"].strip()):
            org_claim["object_identifiers"] = {
                "usgs_web_url": data["organization_link"].strip()
            }
        else:
            org_claim["object_identifiers"] = None
        claims.append(org_claim)

        org_person_claim = {
            "claim_created": claim_stub["claim_created"],
            "claim_source": claim_stub["claim_source"],
            "reference": claim_stub["reference"],
            "property_label": "employs person",
            "date_qualifier": claim_stub["date_qualifier"],
            "object_instance_of": claim_stub["subject_instance_of"],
            "object_label": claim_stub["subject_label"],
            "object_identifiers": claim_stub["subject_identifiers"],
            "subject_instance_of": org_claim["object_instance_of"],
            "subject_label": org_claim["object_label"],
            "subject_identifiers": org_claim["object_identifiers"]
        }
        claims.append(org_person_claim)
    
    if data["expertise"]:
        for expertise_term in data["expertise"]:
            expertise_claim = copy(claim_stub)
            expertise_claim["property_label"] = "has expertise"
            expertise_claim["object_instance_of"] = "UnlinkedTerm"
            expertise_claim["object_label"] = expertise_term.strip()
            claims.append(expertise_claim)

    return id_claims(claims)


def claims_from_orcid(data):
    if "givenName" not in data or "familyName" not in data:
        return

    claims = list()

    claim_stub = {
        "claim_created": str(datetime.utcnow().isoformat()),
        "claim_source": "ORCID",
        "reference": data["@id"],
        "subject_instance_of": "Person",
        "subject_label": f"{data['givenName']} {data['familyName']}",
        "subject_identifiers": {
            "orcid": data["orcid"]
        }
    }

    if "@reverse" in data:
        if "creator" in data["@reverse"]:
            if isinstance(data["@reverse"]["creator"], dict):
                creative_works = [data["@reverse"]["creator"]]
            else:
                creative_works = data["@reverse"]["creator"]

            for item in creative_works:
                author_of_claim = copy(claim_stub)
                author_of_claim["property_label"] = "author of"
                author_of_claim["object_instance_of"] = item["@type"]
                author_of_claim["object_label"] = item["name"]
                if "identifier" in item:
                    author_of_claim["object_identifiers"] = dict()
                    if isinstance(item["identifier"], dict):
                        item["identifier"] = [item["identifier"]]
                    for identifier in item["identifier"]:
                        author_of_claim["object_identifiers"][identifier["propertyID"]] = identifier["value"]
                claims.append(author_of_claim)

                authored_by_claim = {
                    "property_label": "authored by",
                    "claim_created": str(datetime.utcnow().isoformat()),
                    "claim_source": "ORCID",
                    "reference": data["@id"]
                }
                for k,v in author_of_claim.items():
                    if k.split("_")[0] == "subject":
                        authored_by_claim[k.replace('subject', 'object')] = v
                    if k.split("_")[0] == "object":
                        authored_by_claim[k.replace('object', 'subject')] = v
                claims.append(authored_by_claim)

        if "funder" in data["@reverse"]:
            if isinstance(data["@reverse"]["funder"], dict):
                funders = [data["@reverse"]["funder"]]
            else:
                funders = data["@reverse"]["funder"]

            for item in funders:
                funded_by_claim = copy(claim_stub)
                funded_by_claim["property_label"] = "funded by"

                funded_by_claim["object_identifiers"] = dict()
                if "@id" in item:
                    funded_by_claim["object_identifiers"]["fundref_id"] = item["@id"]
                if "identifier" in item:
                    if isinstance(item["identifier"], dict):
                        item["identifier"] = [item["identifier"]]
                    for identifier in item["identifier"]:
                        funded_by_claim["object_identifiers"][identifier["propertyID"]] = identifier["value"]

                funded_by_claim["object_instance_of"] = item["@type"]
                funded_by_claim["object_label"] = item["name"]
                if "alternateName" in item:
                    funded_by_claim["object_label"] = f'{item["name"]}: {item["alternateName"]}'
                claims.append(funded_by_claim)

    if "affiliation" in data:
        if isinstance(data["affiliation"], dict):
            affiliations = [data["affiliation"]]
        else:
            affiliations = data["affiliation"]

        for item in affiliations:
            org_affiliation_claim = copy(claim_stub)
            org_affiliation_claim["property_label"] = "professional affiliation"
            org_affiliation_claim["object_instance_of"] = item["@type"]

            org_affiliation_claim["object_identifiers"] = dict()
            if "@id" in item:
                org_affiliation_claim["object_identifiers"]["fundref_id"] = item["@id"]
            if "identifier" in item:
                if isinstance(item["identifier"], dict):
                    item["identifier"] = [item["identifier"]]
                for identifier in item["identifier"]:
                    org_affiliation_claim["object_identifiers"][identifier["propertyID"]] = identifier["value"]

            org_affiliation_claim["object_label"] = item["name"]
            if "alternateName" in item:
                org_affiliation_claim["object_label"] = f'{item["name"]}: {item["alternateName"]}'
            claims.append(org_affiliation_claim)

            per_affiliation_claim = {
                "property_label": "professional affiliation",
                "claim_created": str(datetime.utcnow().isoformat()),
                "claim_source": "ORCID",
                "reference": data["@id"]
            }
            for k, v in org_affiliation_claim.items():
                if k.split('_')[0] == "subject":
                    per_affiliation_claim[k.replace('subject', 'object')] = v
                if k.split('_')[0] == "object":
                    per_affiliation_claim[k.replace('object', 'subject')] = v
            claims.append(per_affiliation_claim)

    if "alumniOf" in data:
        if isinstance(data["alumniOf"], dict):
            affiliations = [data["alumniOf"]]
        else:
            affiliations = data["alumniOf"]

        for item in affiliations:
            ed_affiliation_claim = copy(claim_stub)
            ed_affiliation_claim["property_label"] = "educational affiliation"
            ed_affiliation_claim["object_instance_of"] = item["@type"]
            ed_affiliation_claim["object_label"] = item["name"]
            if "alternateName" in item:
                ed_affiliation_claim["object_label"] = f'{item["name"]}: {item["alternateName"]}'

            if "identifier" in item:
                ed_affiliation_claim["object_identifiers"] = dict()
                if isinstance(item["identifier"], dict):
                    item["identifier"] = [item["identifier"]]
                for identifier in item["identifier"]:
                    ed_affiliation_claim["object_identifiers"][identifier["propertyID"]] = identifier["value"]

            claims.append(ed_affiliation_claim)

            student_affiliation_claim = {
                "property_label": "educational affiliation",
                "claim_created": str(datetime.utcnow().isoformat()),
                "claim_source": "ORCID",
                "reference": data["@id"]
            }
            for k, v in ed_affiliation_claim.items():
                if k.split('_')[0] == "subject":
                    student_affiliation_claim[k.replace('subject', 'object')] = v
                if k.split('_')[0] == "object":
                    student_affiliation_claim[k.replace('object', 'subject')] = v
            claims.append(student_affiliation_claim)

    return id_claims(claims)


def claims_from_doi(data):
    if isinstance(data["title"], list) and (len(data["container-title"]) == 0 or "reference_string" not in data):
        return

    if isinstance(data["title"], list) and "reference_string" in data:
        item_label = data["reference_string"]
    else:
        item_label = data["title"]

    claims = list()

    claim_reference = {
        "claim_created": str(datetime.utcnow().isoformat()),
        "claim_source": "DOI",
        "reference": data["URL"],
        "date_qualifier": data["issued"]["date-parts"][0][0],
    }

    doi_as_subject = copy(claim_reference)
    doi_as_subject["subject_label"] = item_label
    doi_as_subject["subject_instance_of"] = "CreativeWork"
    doi_as_subject["subject_identifiers"] = {
        "doi": data["DOI"]
    }

    doi_as_object = copy(claim_reference)
    doi_as_object["object_label"] = item_label
    doi_as_object["object_instance_of"] = "CreativeWork"
    doi_as_object["object_identifiers"] = {
        "doi": data["DOI"]
    }

    if "publisher" in data:
        published_by_claim = copy(doi_as_subject)
        published_by_claim["property_label"] = "published by"
        published_by_claim["object_label"] = data["publisher"]
        published_by_claim["object_instance_of"] = "Organization"
        claims.append(published_by_claim)

        published_claim = copy(doi_as_object)
        published_claim["property_label"] = "published"
        published_claim["subject_label"] = data["publisher"]
        published_claim["subject_instance_of"] = "Organization"
        claims.append(published_claim)

    if "container-title" in data and isinstance(data["container-title"], str):
        article_published_in_claim = copy(doi_as_subject)
        article_published_in_claim["property_label"] = "published in"
        article_published_in_claim["object_instance_of"] = "Publication"
        article_published_in_claim["object_label"] = data["container-title"]
        claims.append(article_published_in_claim)

    if "subject" in data:
        for subject in data["subject"]:
            addresses_subject_claim = copy(doi_as_subject)
            addresses_subject_claim["property_label"] = "addresses subject"
            addresses_subject_claim["object_instance_of"] = "UnlinkedTerm"
            addresses_subject_claim["object_label"] = subject.strip()
            claims.append(addresses_subject_claim)

    if "categories" in data:
        for categories_string in data["categories"]:
            for category in categories_string.split(","):
                addresses_subject_claim = copy(doi_as_subject)
                addresses_subject_claim["property_label"] = "addresses subject"
                addresses_subject_claim["object_instance_of"] = "UnlinkedTerm"
                addresses_subject_claim["object_label"] = category.strip()
                claims.append(addresses_subject_claim)

    if "funder" in data:
        for funder in data["funder"]:
            article_funded_by_claim = copy(doi_as_subject)
            article_funded_by_claim["property_label"] = "funded by"
            article_funded_by_claim["object_instance_of"] = "Organization"
            article_funded_by_claim["object_label"] = funder["name"]
            if "DOI" in funder:
                article_funded_by_claim["object_identifiers"] = {
                    "fundref_id": funder["DOI"]
                }
            claims.append(article_funded_by_claim)

    if "event" in data:
        article_event_claim = copy(doi_as_subject)
        article_event_claim["property_label"] = "part of event"
        article_event_claim["object_instance_of"] = "Event"
        article_event_claim["object_label"] = data["event"]
        article_event_claim["object_qualifier"] = "unverified event associated with publication"
        claims.append(article_event_claim)

    party_contacts = list()
    if "author" in data:
        for author in data["author"]:
            author.update({
                "type": "author", 
                "subject_property": "author of", 
                "object_property": "authored by"
            })
        party_contacts.extend(data["author"])

    if "editor" in data:
        for editor in data["editor"]:
            editor.update({
                "type": "editor", 
                "subject_property": "editor of", 
                "object_property": "edited by"
            })
        party_contacts.extend(data["editor"])

    if party_contacts:
        for party in party_contacts:
            if "literal" in party:
                party_label = party["literal"]
                party_instance_of = "Organization"
            elif "name" in party:
                party_label = party["name"]
                party_instance_of = "Organization"
            else:
                if "given" in party and "family" in party:
                    party_label = f"{party['given']} {party['family']}"
                    party_instance_of = "Person"
                elif "family" in party:
                    party_label = party['family']
                    party_instance_of = "Person"

            party_as_object = copy(claim_reference)
            party_as_object["object_instance_of"] = party_instance_of
            party_as_object["object_label"] = party_label
    
            party_as_subject = copy(claim_reference)
            party_as_subject["subject_instance_of"] = party_instance_of
            party_as_subject["subject_label"] = party_label

            if "sequence" in party:
                party_as_object["object_qualifier"] = f'{party["sequence"]} {party["type"]}'
                party_as_subject["subject_qualifier"] = f'{party["sequence"]} {party["type"]}'

            if "ORCID" in party:
                party_identifiers = utilities.actionable_id(party["ORCID"])
                if party_identifiers is not None:
                    party_as_object["object_identifiers"] = party_identifiers
                    party_as_subject["subject_identifiers"] = party_identifiers
            
            contributed_by_claim = copy(doi_as_subject)
            contributed_by_claim["property_label"] = party["object_property"]
            contributed_by_claim.update(party_as_object)
            claims.append(contributed_by_claim)

            contributor_claim = copy(doi_as_object)
            contributor_claim["property_label"] = party["subject_property"]
            contributor_claim.update(party_as_subject)
            claims.append(contributor_claim)

            if "affiliation" in party:
                for affiliation in party["affiliation"]:
                    party_affiliated_with_claim = copy(party_as_subject)
                    party_affiliated_with_claim["property_label"] = "professional affiliation"
                    party_affiliated_with_claim["object_instance_of"] = "Organization"
                    party_affiliated_with_claim["object_label"] = affiliation["name"]
                    claims.append(party_affiliated_with_claim)

                    org_affiliated_with_claim = copy(party_as_object)
                    org_affiliated_with_claim["property_label"] = "professional affiliation"
                    org_affiliated_with_claim["subject_instance_of"] = "Organization"
                    org_affiliated_with_claim["subject_label"] = affiliation["name"]
                    claims.append(org_affiliated_with_claim)

            if "container-title" in data and isinstance(data["container-title"], str):
                party_published_in_claim = copy(party_as_subject)
                party_published_in_claim["property_label"] = "published in"
                party_published_in_claim["object_instance_of"] = "Publication"
                party_published_in_claim["object_label"] = data["container-title"]
                claims.append(party_published_in_claim)

            if "subject" in data:
                for subject in data["subject"]:
                    party_addresses_subject_claim = copy(party_as_subject)
                    party_addresses_subject_claim["property_label"] = "addresses subject"
                    party_addresses_subject_claim["object_instance_of"] = "UnlinkedTerm"
                    party_addresses_subject_claim["object_label"] = subject.strip()
                    claims.append(party_addresses_subject_claim)

            if "categories" in data:
                for categories_string in data["categories"]:
                    for category in categories_string.split(","):
                        party_addresses_subject_claim = copy(party_as_subject)
                        party_addresses_subject_claim["property_label"] = "addresses subject"
                        party_addresses_subject_claim["object_instance_of"] = "UnlinkedTerm"
                        party_addresses_subject_claim["object_label"] = category.strip()
                        claims.append(party_addresses_subject_claim)

            if "funder" in data:
                for funder in data["funder"]:
                    party_funded_by_claim = copy(party_as_subject)
                    party_funded_by_claim["property_label"] = "funded by"
                    party_funded_by_claim["object_instance_of"] = "Organization"
                    party_funded_by_claim["object_label"] = funder["name"]
                    party_funded_by_claim["object_qualifier"] = "funding reported from published article"
                    if "DOI" in funder:
                        party_funded_by_claim["object_identifiers"] = {
                            "fundref_id": funder["DOI"]
                        }
                    claims.append(party_funded_by_claim)

            if "event" in data:
                party_event_claim = copy(party_as_subject)
                party_event_claim["property_label"] = "participated in event"
                party_event_claim["object_instance_of"] = "Event"
                party_event_claim["object_label"] = data["event"]
                party_event_claim["object_qualifier"] = "unverified event associated with publication"
                claims.append(party_event_claim)

    return id_claims(claims)


def claims_from_sb_person(data):
    claims = list()

    claim_reference = {
        "claim_created": str(datetime.utcnow().isoformat()),
        "claim_source": "ScienceBase Directory",
        "reference": data["link"]["href"],
    }

    if "last_updated" in data:
        claim_reference["date_qualifier"] = data["last_updated"]
    else:
        claim_reference["date_qualifier"] = data["_date_cached"]

    person_identifiers = {
        "sbid": data["link"]["href"]
    }

    if "email" in data:
        person_identifiers["email"] = data["email"]

    if "identifiers" in data:
        for identifier_item in data["identifiers"]:
            person_identifiers[identifier_item["type"].lower()] = identifier_item["key"]

    subject_claim = copy(claim_reference)
    object_claim = copy(claim_reference)

    subject_claim["subject_instance_of"] = "Person"
    subject_claim["subject_label"] = data["displayName"]
    subject_claim["subject_identifiers"] = person_identifiers
    object_claim["object_instance_of"] = "Person"
    object_claim["object_label"] = data["displayName"]
    object_claim["object_identifiers"] = person_identifiers

    for k,v in person_identifiers.items():
        subject_claim[f"subject_identifier_{k}"] = v
        object_claim[f"object_identifier_{k}"] = v

    if "jobTitle" in data:
        job_title_claim = copy(subject_claim)
        job_title_claim["property_label"] = "job title"
        job_title_claim["object_instance_of"] = "FieldOfWork"
        job_title_claim["object_label"] = data["jobTitle"]
        claims.append(job_title_claim)

    try:
        org_info = data["extensions"]["personExtension"]["organization"]
    except:
        org_info = None

    try:
        usgs_person = data["extensions"]["usgsPersonExtension"]
    except:
        usgs_person = None

    if org_info is not None:
        org_affiliation_claim = copy(subject_claim)
        org_affiliation_claim["property_label"] = "employed by"
        org_affiliation_claim["object_instance_of"] = "Organization"
        org_affiliation_claim["object_label"] = org_info["displayText"]
        org_affiliation_claim["object_identifiers"] = {
            "sbid": f"https://www.sciencebase.gov/directory/organization/{org_info['id']}"
        }
        if usgs_person is not None:
            org_affiliation_claim["object_identifiers"]["fbms_code"] = usgs_person["orgCode"]

        if "active" in data:
            if data["active"]:
                org_affiliation_claim["subject_qualifier"] = "Current active employee as of date_qualifier"
            else:
                org_affiliation_claim["subject_qualifier"] = "No longer an active employee"
        claims.append(org_affiliation_claim)

        person_affiliation_claim = copy(object_claim)
        person_affiliation_claim["property_label"] = "employs person"
        person_affiliation_claim["subject_instance_of"] = "Organization"
        person_affiliation_claim["subject_label"] = org_info["displayText"]
        person_affiliation_claim["subject_identifiers"] = {
            "sbid": f"https://www.sciencebase.gov/directory/organization/{org_info['id']}"
        }
        if usgs_person is not None:
            person_affiliation_claim["subject_identifiers"]["fbms_code"] = usgs_person["orgCode"]

        if "active" in data:
            if data["active"]:
                person_affiliation_claim["object_qualifier"] = "Current active employee as of date_qualifier"
            else:
                person_affiliation_claim["object_qualifier"] = "No longer an active employee"
        claims.append(person_affiliation_claim)

    if "primaryLocation" in data and "streetAddress" in data["primaryLocation"]:
        work_location_claim = copy(subject_claim)
        work_location_claim["property_label"] = "work location"
        work_location_claim["object_instance_of"] = "UnverifiedLocation"
        work_location_claim["object_label"] = ", ".join([i for i in data["primaryLocation"]["streetAddress"].values() if i is not None])
        claims.append(work_location_claim)

    return id_claims(claims)


def entity_from_claims(claims, target_instance_of, source_doc=None):
    if target_instance_of not in summarization_properties.keys():
        return

    config = summarization_properties[target_instance_of]

    entity = {
        "entity_updated": str(datetime.utcnow().isoformat()),
        "instance_of": target_instance_of,
        "identifiers": dict(),
        "references": list(set([i["reference"] for i in claims])),
        "sources": list(set([i["claim_source"] for i in claims])),
        "category": config["category"]["default"]
    }

    all_names = list(set(
            [i["subject_label"] for i in claims if i["subject_instance_of"] == target_instance_of]
            +
            [i["object_label"] for i in claims if i["object_instance_of"] == target_instance_of]
        ))

    if target_instance_of == "Person":
        entity["alternate_names"] = all_names

    subject_identifiers = [
        i["subject_identifiers"] for i in claims 
        if "subject_identifiers" in i and i["subject_instance_of"] == target_instance_of
    ]
    object_identifiers = [
        i["object_identifiers"] for i in claims 
        if "object_identifiers" in i and i["object_instance_of"] == target_instance_of
    ]
    all_identifiers = subject_identifiers + object_identifiers

    for identifiers in all_identifiers:
        for k,v in identifiers.items():
            if k not in entity["identifiers"]:
                entity["identifiers"][k] = v
                entity[f"identifier_{k}"] = v

    if "name" in entity["identifiers"]:
        entity["name"] = entity["identifiers"]["name"]
    else:
        entity["name"] = all_names[0]

    for prop in config["facets"]:
        entity[prop] = list(set([
            i["object_label"] for i in claims 
            if i["property_label"] == prop 
            and i["subject_instance_of"] == target_instance_of
        ]))

    if source_doc is not None:
        for prop in summarization_properties[target_instance_of]["properties_from_source"]:
            if prop in source_doc:
                entity[prop] = source_doc[prop]

        if "source prop" in config["category"] and config["category"]["source prop"] in source_doc:
            mapped_category = next(
                (
                    v for k,v in config["category"]["mapping"].items()
                    if k == source_doc[config["category"]["source prop"]]
                ), None)
            if mapped_category is not None:
                entity["category"] = mapped_category

    if "linkable_id" in config and config["linkable_id"]["property"] in entity["identifiers"]:
        entity["link"] = f'{config["linkable_id"]["resolver"]}{entity["identifiers"][config["linkable_id"]["property"]]}'

    for identifier_type in ["email","orcid","doi"]:
        if f"identifier_{identifier_type}" in entity:
            identifier = entity[f"identifier_{identifier_type}"]
            entity["entity_id"] = f"{identifier_type}_{hashlib.md5(identifier.encode('utf-8')).hexdigest()}"
            break

    return entity


summarization_properties = {
    "Person": {
        "facets": [
            "job title",
            "educational affiliation",
            "professional affiliation",
            "employed by",
            "has expertise",
            "addresses subject",
            "author of",
            "editor of",
            "published in",
            "funded by",
            "participated in event"
        ],
        "category": {
            "default": "person"
        }
    },
    "CreativeWork": {
        "facets": [
            "authored by",
            "edited by",
            "addresses subject",
            "part of event",
            "published in",
            "published by"
        ],
        "properties_from_source": ["abstract"],
        "category": {
            "type": "derived from source",
            "default": "document",
            "source prop": "type",
            "mapping": {
                'dataset': 'data'
            }
        },
        "linkable_id": {
            "property": "doi",
            "resolver": "https://doi.org/"
        }
    }
}

