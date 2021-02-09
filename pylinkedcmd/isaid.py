from datetime import datetime
import validators
from copy import copy
import hashlib
from . import utilities

def id_claims(claims, include_uid=True):
    for claim in claims:
        claim["claim_id"] = ":".join([
            claim["claim_source"],
            claim["subject_label"],
            claim["property_label"],
            claim["object_label"]
        ])
        if include_uid:
            claim["claim_uid"] = hashlib.md5(claim["claim_id"].encode('utf-8')).hexdigest()

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
            org_affiliation_claim["property_label"] = "organization affiliation"
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
                "property_label": "personnel affiliation",
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
                "property_label": "student affiliation",
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
