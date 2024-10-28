import requests
import time
import random
from joblib import Parallel, delayed

geonames_users = [] #lista loginów zarejestrowanych użytkowników 

def query_geonames(m):
    url = 'http://api.geonames.org/searchJSON?'
    params = {'username': random.choice(geonames_users), 'q': m, 'featureClass': 'P', 'style': 'FULL', 'name_equals': m, 'isNameRequired': 'true'}

    for _ in range(3):  
        result = requests.get(url, params=params).json()
        if 'status' in result:
            time.sleep(5)
            continue
        else:
            geonames_resp = []
            for e in result.get('geonames', []):
                if e['name'].lower() == m.lower() or any(alternate_name['name'].lower() == m.lower() for alternate_name in e.get('alternateNames', [])):
                    geonames_resp.append([e['geonameId'], e['name'], e['lat'], e['lng']])
            return geonames_resp
    return None

def query_geonames_joblib(terms):
    results = Parallel(n_jobs=-1)(delayed(query_geonames)(term) for term in terms)
    return {term: result for term, result in zip(terms, results)}
