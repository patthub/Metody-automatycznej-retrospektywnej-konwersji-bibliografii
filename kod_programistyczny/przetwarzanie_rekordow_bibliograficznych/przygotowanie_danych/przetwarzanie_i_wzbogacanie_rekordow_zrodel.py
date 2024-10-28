# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
from tqdm import tqdm
import regex as re
import json
import requests
import Levenshtein
from tqdm import tqdm
import os


def read_file(path):
	with open(path, "r", encoding = "utf-8") as f:
		data = f.readlines()
	return data


def extract_bibliographic_source(line: str) -> list:

    try:
        single_bibliographic_source = {}
        temp = re.split("-|—", line)
        temp_value = re.split("\.", temp[1])[0]
        temp_value = re.sub("za rok \d+", "",  temp_value)
        temp_value = re.sub("\d+", "",  temp_value).strip()
        single_bibliographic_source["Abbreviation"] = temp[0].strip()
        single_bibliographic_source["Name"] = temp_value.strip()

        return single_bibliographic_source
    except IndexError:
        pass

def extract_abbreviation(line: str) -> list:

    try:
        single_abbreviation = {}
        temp = re.split("-", line)
        single_abbreviation[temp[0].strip()] = temp[1].strip()
        return single_abbreviation
    except IndexError:
        pass


def get_data(k: str, v: str) -> list:

    BASE_BN_URL = "http://data.bn.org.pl/api/institutions/bibs.json?marc=245a"

    response_main = {}
    url = f"http://data.bn.org.pl/api/institutions/bibs.json?marc=245a+{v}"

    responses = []
    try:
        while url:
            url = requests.get(url)
            if url.status_code == 200:
                url = url.json()
                responses.append(url)
                url = url["nextPage"]
                print(f"Downloading: {url}, {v}")
            else:
                print("Error while accessing API")
        print("Download complete")
        response = responses[0]
        print(url)
        if len(response["bibs"]) > 0:

            response = dict({(k , v) for k, v in response["bibs"][0].items() if k in["author", "title", "id", "genre"]})
            response_main[k] = (v, response)
            return response_main
        else:
            print("0 records found")
    except IndexError:
        pass



def get_data_for_bibliographic_source(v="No data") -> list:
    BASE_BN_URL = "http://data.bn.org.pl/api/institutions/bibs.json?marc=245a"
    response_main = {}
    if len(v) < 2 or v is None:
        v = "Error, data not found"

    url = f"http://data.bn.org.pl/api/institutions/bibs.json?formOfWork=Czasopisma&marc=245a+{v}"
    responses = []
    max_pages = 10  
    current_page = 0

    try:
        while url and current_page < max_pages:
            response = requests.get(url)
            if response.status_code == 200:
                response_json = response.json()
                responses.append(response_json)
                url = response_json.get("nextPage")
                current_page += 1
                print(f"Przetwarzanie strony {current_page}: {url}")
            else:
                print("Error while accessing API")
                break

        final_responses = []
        for response in responses:
            if len(response["bibs"]) > 0:
                final_responses.append(response)
            else:
                print("0 records found")

        return final_responses

    except IndexError:
        print("Błąd: IndexError")
        return []



def magic_add_to_dict(key, value, dest):
    if key in dest:
        dest[key].append(value)
    else:
        dest[key] = [value]



def create_bibliographic_source_record(pbl_record):
    try:
        temp = get_data_for_bibliographic_source(pbl_record.get("Name"))
        if temp and temp != "*":
            hits = []
            title_from_pbl = ''.join(e.lower() for e in pbl_record.get("Name") if e.isalnum())

            for elem in temp[:10]:
                for x in elem["bibs"]:
                    for field in x["marc"]["fields"]:
                        if "245" in field:
                            original_title = field["245"]["subfields"][0].get("a")
                            original_title_normalized = ''.join(e.lower() for e in original_title if e.isalnum())
                            levenshtein_ratio = Levenshtein.ratio(title_from_pbl, original_title_normalized)
                            hits.append({
                                "title_for_comparison": original_title_normalized,
                                "whole_rec": x["marc"]["fields"],
                                "Levenshtein": (levenshtein_ratio, title_from_pbl, original_title_normalized)
                            })
                            # Przerwij, jeśli znajdziesz idealne dopasowanie
                            if levenshtein_ratio == 1.0:
                                break

            quality_hits = [hit for hit in hits if hit["Levenshtein"][0] >= 0.9]
            quality_hits.sort(key=lambda x: x["Levenshtein"][0], reverse=True)

            if quality_hits:
                final_hit = quality_hits[0]
                final_hit_filtered = {}
                for field in final_hit.get("whole_rec"):
                    if "009" in field:
                        final_hit_filtered["bn_id"] = field.get("009")
                    elif "130" in field:
                        final_hit_filtered["bn_unified_title"] = field.get("130").get("subfields")[0].get("a")
                    elif "245" in field:
                        final_hit_filtered["bn_title"] = field.get("245").get("subfields")[0].get("a")
                    elif "246" in field:
                        final_hit_filtered["246_title"] = field.get("246").get("subfields")[0].get("a")
                    elif "380" in field:
                        magic_add_to_dict("bn_form_of_work", field.get("380").get("subfields")[0].get("a"), final_hit_filtered)
                    elif "650" in field:
                        magic_add_to_dict("bn_subjects", field.get("650").get("subfields")[0].get("a"), final_hit_filtered)
                    elif "655" in field:
                        magic_add_to_dict("bn_genre_form", field.get("655").get("subfields")[0].get("a"), final_hit_filtered)
                    elif "700" in field:
                        magic_add_to_dict(
                            "bn_secondary_author",
                            (field.get("700").get("subfields")[0].get("a"), field.get("700").get("subfields")[-1].get("e")),
                            final_hit_filtered
                        )
                pbl_record["BN_INFO"] = final_hit_filtered
            return pbl_record
        else:
            print("Brak rekordu")
    except AttributeError:
        print("Nie znaleziono rekordu")
    except TypeError:
        print("Brak danych")

def process_multiple_txt_files(directory_path):

    all_records = []
    abbreviations_seen = set()
    total_duplicates = 0

    for filename in os.listdir(directory_path):
        if filename.endswith(".txt"):
            file_path = os.path.join(directory_path, filename)
            lines = read_file(file_path)
            print(f"Plik: {filename}, liczba wierszy: {len(lines)}")
            
            for line in lines:
                bibliographic_source = extract_bibliographic_source(line)
                if bibliographic_source and bibliographic_source["Abbreviation"] not in abbreviations_seen:
                    all_records.append(bibliographic_source)
                    abbreviations_seen.add(bibliographic_source["Abbreviation"])
                else:
                    total_duplicates += 1

    deduplicated_output_file = "deduplicated_bibliographic_sources.json"
    with open(deduplicated_output_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=4)

    print(f"Deduplicated plik JSON został zapisany jako {deduplicated_output_file}")
    print(f"Liczba rekordów w deduplikowanym pliku: {len(all_records)}")
    print(f"Liczba rekordów zduplikowanych: {total_duplicates}")

    final_records = []
    for record in tqdm(all_records):
        processed_record = create_bibliographic_source_record(record)
        if processed_record:
            final_records.append(processed_record)

    final_output_file = "final_bibliographic_sources.json"
    with open(final_output_file, "w", encoding="utf-8") as f:
        json.dump(final_records, f, ensure_ascii=False, indent=4)

    print(f"Plik JSON z informacjami z API został zapisany jako {final_output_file}")
    print(f"Liczba rekordów w pliku końcowym: {len(final_records)}")
    print(f"Liczba wierszy w pliku końcowym: {os.path.getsize(final_output_file)} bajtów")

#directory_path = r"C:\Users\patry\Desktop\retro\PBL_RAW_NEW_OCR\TXT\czesc_wstepna"
#process_multiple_txt_files(directory_path)
