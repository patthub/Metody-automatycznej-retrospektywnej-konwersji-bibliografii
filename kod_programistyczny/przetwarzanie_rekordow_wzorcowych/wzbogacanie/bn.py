import pandas as pd
import numpy as np
import requests
import json
from tqdm import tqdm
from joblib import Parallel, delayed



field_mapping = {
  
    "person": "marc=100a+",
    "organization": "marc=110a+",
    "event": "marc=111a+",
    "place": "marc=151a+",
    "subject": "marc=150a+"
}


def query_data_bn_authorities(term, field):
  


    BASE_BN_URL = f"http://data.bn.org.pl/api/institutions/authorities.json?{field}"
    url = BASE_BN_URL + term
    responses = []
    while url:
        response = requests.get(url)
        if response.status_code == 200:
            response_json = response.json()
            responses.append(response_json)
            url = response_json.get("nextPage", None)
        else:
            print("Error while accessing API")
            break
    return responses

def query_omnis(term, query_endpoint):
    BASE_BN_URL = f"https://omnis.bn.org.pl/api/work/list?metadata%5B0%5D.label={query_endpoint}&metadata%5B0%5D.value={term}"
    url = BASE_BN_URL
    responses = []
    while url:
        response = requests.get(url)
        if response.status_code == 200:
            response_json = response.json()
            responses.append(response_json)
            url = None  # Zmiana, ponieważ brak paginacji w API
        else:
            print("Error while accessing API")
            break
    return responses

def get_record_id(record):
    for field in record[0][0]["bibs"][0]["marc"]["fields"]:
        for k, v in field.items():
            if k == "009":
                return v
    return None

def query_omnis_by_id(record):
    id = get_record_id(record)
    if id:
        BASE_BN_URL = f"https://omnis.bn.org.pl/api/work/list?metadata%5B0%5D.label=search_identity&metadata%5B0%5D.value={id}"
        response = requests.get(BASE_BN_URL.strip())
        if response.status_code == 200:
            return response.json()
        else:
            print("Error while accessing API")
    return None

def query_data_bn_authorities_joblib(terms, field):
    results = Parallel(n_jobs=-1)(delayed(query_data_bn_authorities)(term, field) for term in terms)
    return results

def query_omnis_by_id_joblib(records):
    results = Parallel(n_jobs=-1)(delayed(query_omnis_by_id)(record) for record in records)
    return results


import re
from tqdm import tqdm
import json
import requests
import jellyfish


def split_line_into_records(line):
    # Wzór do wykrywania nazwisk i numerów, uwzględniający złożone nazwiska
    pattern = r'\b[A-ZĄĆĘŁŃÓŚŹŻ][a-ząćęłńóśźż]+(?:[- ][A-ZĄĆĘŁŃÓŚŹŻ][a-ząćęłńóśźż]+)* \d+(?:[-,] ?\d+)*'
    return re.findall(pattern, line)

def process_file(filename):
    result = {}
    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            records = split_line_into_records(line)
            for record in records:
                # Split name and nums
                nums = re.findall(r'\d+(?:[-,] ?\d+)*', record)
                result.setdefault(record, []).extend(nums)
    return result


def remove_numbers_from_key(dictionary):
    new_dictionary = {}
    for key, values in dictionary.items():
        # Splitting the name from the numbers in the key
        name, numbers = key.split(maxsplit=1)
        # Converting the string of numbers in the key and in the list to sets
        numbers_in_key = set(numbers.replace(',', '').split())
        numbers_in_list = set(values[0].split(', '))
        # Removing numbers from the key that are in the list
        final_numbers = numbers_in_key - numbers_in_list
        # Creating a new key
        new_key = f"{name} {' '.join(final_numbers)}"
        new_dictionary[new_key] = values
    return new_dictionary

def dict_to_list(dictionary):
    list_of_dicts = []
    for name, nums in dictionary.items():
        # Creating a new dictionary and adding it to the list
        list_of_dicts.append({'name': name.strip(), 'nums': nums})
    return list_of_dicts


def save_list_to_json_file(list_data, file_name):
    # List to json file
    with open(file_name, 'w', encoding='utf-8') as file:
        json.dump(list_data, file, ensure_ascii=False, indent=4)


def query_and_compare_bn_data(term, field):

    BASE_BN_URL = f"http://data.bn.org.pl/api/institutions/authorities.json?{field}"
    url = BASE_BN_URL + term
    responses = []
    all_records = []
    similar_records = []
    pages_fetched = 0
    max_pages = 2  # Limit to 2 pages

    # Query BN database using session
    while url and pages_fetched < max_pages:
        response = session.get(url)
        if response.status_code == 200:
            response_json = response.json()
            responses.append(response_json)
            url = response_json.get("nextPage", None)
            pages_fetched += 1
        else:
            print("Error while accessing API")
            break

    # Extract and process data
    for record in responses:
        for elem in record["authorities"]:
            new_rec = {}
            if elem["title"] == "":
                for field in elem["marc"]["fields"]:
                    if "100" in field:
                        new_rec["100"] = field["100"]
                        # Extract birth and death dates
                        if 'subfields' in field["100"]:
                            for subfield in field["100"]["subfields"]:
                                if 'd' in subfield:
                                    dates = subfield['d'].split('-')
                                    if len(dates) > 0:
                                        new_rec['birth'] = dates[0].strip().replace("(", "").replace(")","").strip()
                                    if len(dates) > 1:
                                        new_rec['death'] = dates[1].strip().replace("(", "").replace(")","").strip()
                    elif "024" in field:
                        new_rec["024"] = field["024"]
                    elif "035" in field:
                        new_rec["035"] = field["035"]
                    elif "009" in field:
                        new_rec["009"] = field["009"]
                all_records.append(new_rec)

    # Check if only one record is found
    if len(all_records) == 1:
        single_record = all_records[0].copy()
        single_record['similarity'] = 1.0
        return [single_record]

    # Compare with input term using Jaro-Winkler
    for record in all_records:
        if '100' in record and 'subfields' in record['100']:
            for subfield in record['100']['subfields']:
                if 'a' in subfield:
                    name = subfield['a']
                    similarity = jellyfish.jaro_winkler_similarity(term, name)
                    if similarity > 0.95:  # Treshold of Jaro-Winkler metric
                        similar_record = record.copy()
                        similar_record['similarity'] = similarity
                        similar_records.append(similar_record)

    return similar_records if similar_records else all_records


def generate_ids(data, base_id):
    base_url, base_num = base_id.rsplit('/', 1)
    current_num = int(base_num[1:])  # Pomijamy pierwszy znak, który jest typem rekordu

    modified_data = []
    for item in data:
        modified_item = item.copy()  # Tworzymy kopię każdego słownika
        item_id = f"{base_url}/a1{str(current_num).zfill(17)}"
        modified_item['id'] = item_id
        modified_data.append(modified_item)
        current_num += 1

    return modified_data
