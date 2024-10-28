import requests
import Levenshtein as lv
import re
from joblib import Parallel, delayed

def read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        data = f.readlines()
    return data

indeks_raw = read_file("/content/drive/MyDrive/Doktorat/Datasets/1972.txt")

def search_viaf(query):
    url = "https://www.viaf.org/viaf/AutoSuggest"
    params = {"query": query}

    response = requests.get(url, params=params)

    if response.status_code == 200:
        results = response.json().get("result", [])
        return results
    else:
        print(f"Error {response.status_code}: {response.text}")
        return []

def extract_name_without_dates(label):
    name = re.sub(r'\d{4}-\d{4}', '', label)  # usuwa datę w formacie YYYY-YYYY
    name = re.sub(r'\d{4}-', '', name)        # usuwa datę w formacie YYYY-
    name = re.sub(r'\d{4}', '', name)         # usuwa datę w formacie YYYY
    return name.strip()

def find_most_similar(query, results):
    most_similar = None
    highest_similarity = -1

    for result in results:
        label = result["displayForm"]
        name_without_dates = extract_name_without_dates(label)
        similarity = lv.ratio(query.lower(), name_without_dates.lower())

        if similarity > highest_similarity:
            highest_similarity = similarity
            most_similar = {
                "id": result["viafid"],
                "label": label,
                "similarity": similarity
            }

    return most_similar

def process_name(name):
    results = search_viaf(name)
    if results:
        return find_most_similar(name, results)
    return None

def reorder_name(name):
    name_parts = name.split()
    if len(name_parts) == 2:
        return f"{name_parts[1]} {name_parts[0]}"
    elif len(name_parts) > 2:
        return f"{' '.join(name_parts[1:])} {name_parts[0]}"
    return name

def query_viaf_joblib(names_list):
    results = Parallel(n_jobs=-1)(delayed(process_name)(name) for name in names_list)
    return {name: result for name, result in zip(names_list, results)}

names_list = [reorder_name(name.strip()) for name in indeks_raw]
results_dict = query_viaf_joblib(names_list)
