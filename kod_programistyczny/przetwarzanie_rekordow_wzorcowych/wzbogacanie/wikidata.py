from SPARQLWrapper import SPARQLWrapper, JSON


def query_wikidata_without_instance(term):

'''
Funkcja przeznaczona do wykorzystania w przypadku wzbogacania haseł bez określenia typu
'''
    try:
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        query = """
            SELECT distinct ?item ?itemLabel ?label_en ?label_fr ?label_ger 
            ?label_ukr ?instance ?instanceLabel ?itemDescription ?lcsh ?unesco ?eurovoc ?YSO ?openalex ?viaf

            WHERE{  

              ?item ?label "%s"@pl .  
              OPTIONAL {?item wdt:P31 ?instance . }
              OPTIONAL {?item wdt:P214 ?viaf . }
              OPTIONAL {?item wdt:P244 ?lcsh . }
              OPTIONAL {?item wdt:P757 ?unesco . }
              OPTIONAL {?item wdt:P5437 ?eurovoc . }
              OPTIONAL {?item wdt:P10283 ?openalex . }
              OPTIONAL {?item wdt:P2347 ?YSO . }
              OPTIONAL {?item rdfs:label ?label_en filter (lang(?label_en) = "en"). }
              OPTIONAL {?item rdfs:label ?label_fr filter (lang(?label_fr) = "fr"). }
              OPTIONAL {?item rdfs:label ?label_ger filter (lang(?label_ger) = "de"). }
              OPTIONAL {?item rdfs:label ?label_ukr filter (lang(?label_ukr) = "uk"). }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }    
            }
        """ % (term)
        sparql.setQuery(query)

        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()


        simple_results = []
        for binding in results['results']['bindings']:
            simple_result = {k: v['value'] for k, v in binding.items() if 'value' in v}
            simple_results.append(simple_result)

        return simple_results

    except Exception as e:
        print(f"Error: {e}")
        return []



def query_wikidata(instance_type, term):

'''
Funkcja przeznaczona do wykorzystania w przypadku wzbogacania osób, organizacji oraz miejsc
'''
    try:
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        query = """
            SELECT distinct ?item ?itemLabel ?label_en ?label_fr ?label_ger 
            ?label_ukr ?instance ?instanceLabel ?itemDescription ?lcsh ?unesco ?eurovoc ?YSO ?openalex ?viaf

            WHERE{  
              ?item ?instance wd:%s . 
              ?item ?label "%s"@pl .  
              OPTIONAL {?item wdt:P31 ?instance . }
              OPTIONAL {?item wdt:P214 ?viaf . }
              OPTIONAL {?item wdt:P244 ?lcsh . }
              OPTIONAL {?item wdt:P757 ?unesco . }
              OPTIONAL {?item wdt:P5437 ?eurovoc . }
              OPTIONAL {?item wdt:P10283 ?openalex . }
              OPTIONAL {?item wdt:P2347 ?YSO . }
              OPTIONAL {?item rdfs:label ?label_en filter (lang(?label_en) = "en"). }
              OPTIONAL {?item rdfs:label ?label_fr filter (lang(?label_fr) = "fr"). }
              OPTIONAL {?item rdfs:label ?label_ger filter (lang(?label_ger) = "de"). }
              OPTIONAL {?item rdfs:label ?label_ukr filter (lang(?label_ukr) = "uk"). }
              SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }    
            }
        """ % (instance_type, term)
        sparql.setQuery(query)

        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()


        simple_results = []
        for binding in results['results']['bindings']:
            simple_result = {k: v['value'] for k, v in binding.items() if 'value' in v}
            simple_results.append(simple_result)

        return simple_results

    except Exception as e:
        print(f"Error: {e}")
        return []


type_mapping = {
  
    "person": "Q5",
    "organization": "Q43229",
    "city": "Q515",
    "village": "Q532"
}

def query_wikidata_concept(label):

'''
Funkcja przeznaczona do wykorzystania w przypadku wzbogacania haseł przedmiotowych
'''
    def execute_query(label):
        query = f"""
        SELECT ?item ?itemId ?plLabel ?enLabel ?frLabel ?deLabel ?ukLabel ?lcsh ?yso ?eurovoc ?openaire ?openalex
        WHERE {{
            ?item ?label "{label}"@pl.
            BIND(STRAFTER(STR(?item), "http://www.wikidata.org/entity/") AS ?itemId)
            FILTER(STRSTARTS(?itemId, "Q"))
            OPTIONAL {{?item rdfs:label ?plLabel FILTER (LANG(?plLabel) = "pl")}}
            OPTIONAL {{?item rdfs:label ?enLabel FILTER (LANG(?enLabel) = "en")}}
            OPTIONAL {{?item rdfs:label ?frLabel FILTER (LANG(?frLabel) = "fr")}}
            OPTIONAL {{?item rdfs:label ?deLabel FILTER (LANG(?deLabel) = "de")}}
            OPTIONAL {{?item rdfs:label ?ukLabel FILTER (LANG(?ukLabel) = "uk")}}
            OPTIONAL {{?item wdt:P244 ?lcsh .}}
            OPTIONAL {{?item wdt:P2347 ?yso .}}
            OPTIONAL {{?item wdt:P5061 ?eurovoc .}}
            OPTIONAL {{?item wdt:P7915 ?openaire .}}
            OPTIONAL {{?item wdt:P7520 ?openalex .}}
        }}
        """

        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        simple_results = []
        for binding in results['results']['bindings']:
            simple_result = {k: v['value'] for k, v in binding.items() if 'value' in v}
            simple_results.append(simple_result)

        return simple_results

    num_words = len(label.split())

    if num_words > 1:
        results = execute_query(label)
        if not results:
            label_lowercase = label.lower()
            results = execute_query(label_lowercase)
    else:
        label_lowercase = label.lower()
        results = execute_query(label_lowercase)
        if not results:
            label_first_letter_upper = label_lowercase[0].upper() + label_lowercase[1:]
            results = execute_query(label_first_letter_upper)

    return results
