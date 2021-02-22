import requests
from datetime import datetime
import math

publication_api = "https://pubs.er.usgs.gov/pubs-services/publication"

def pw_records(q=None, author_id=None, mod_x_days=None, publication_year=None, page_size=1000):
    query_url = f"{publication_api}/?page_size={page_size}"
    if q is not None:
        query_url = f"{query_url}&q={q}"
    if author_id is not None:
        query_url = f"{query_url}&contributor={author_id}"
    if mod_x_days is not None:
        query_url = f"{query_url}&mod_x_days={mod_x_days}"
    if publication_year is not None:
        query_url = f"{query_url}&startYear={str(publication_year)}&endYear={str(publication_year)}"

    r = requests.get(query_url)

    if r.status_code != 200:
        return {
            "query_url": query_url,
            "error": f"HTTP error: {r.headers}"
        }

    response_data = r.json()

    if "recordCount" not in response_data.keys():
        return {
            "query_url": query_url,
            "error": "No results in response"
        }

    records = response_data["records"]
    
    if response_data["recordCount"] > page_size:
        last_page_number = math.ceil(response_data["recordCount"] / page_size)
        for page_num in range(1, last_page_number):
            r = requests.get(f"{query_url}&page_number={page_num}")
            if r.status_code != 200:
                break
            response_data = r.json()
            if response_data["records"]:
                records.extend(response_data["records"])

    for record in records:
        record.update({"_date_cached": str(datetime.utcnow().isoformat())})

    return records


