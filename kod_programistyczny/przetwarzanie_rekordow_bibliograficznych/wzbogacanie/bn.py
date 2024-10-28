def magic_add_to_dict(key, value, dest):
    if key in dest:
        dest[key].append(value)
    else:
        dest[key] = [value]

      

def create_bibliographic_source_record(pbl_record):
    
    try:
        temp = get_data_for_bibliographic_source(pbl_record.get("Name"))
        hits = [] 
        single_rec = {}
        if temp != None and temp != "*":
            for elem in temp:
                for x in elem["bibs"]:
                    for field in x["marc"]["fields"]:
                        if "245" in field:
                            original_title = field["245"]["subfields"][0].get("a")
                            original_title = ''.join(e.lower() for e in original_title if e.isalnum())
                            single_rec["title_for_comparison"] = original_title
                            single_rec["whole_rec"] = x["marc"]["fields"]
                            hits.append(single_rec)
                            single_rec = {}

            quality_hits = []

            title_from_pbl = ''.join(e.lower() for e in pbl_record.get("Name") if e.isalnum())
            for record in hits:
                record["Levenshtein"] = (Levenshtein.ratio(title_from_pbl, record["title_for_comparison"]),title_from_pbl, record["title_for_comparison"])


            for hit in hits:
                if hit.get("Levenshtein")[0] == 1.0:
                    quality_hits.append(hit)
                    break
                elif hit.get("Levenshtein")[0] > 0.9:
                    quality_hits.append(hit)

            if len(quality_hits) > 0:

                final_hit = quality_hits[0]

                final_hit_filtered = {}
                for field in final_hit.get("whole_rec"):
                    if "009" in field:
                        final_hit_filtered["bn_id"] = field.get("009")
                    elif "130" in field:
                        final_hit_filtered["bn_unified_title"] = field.get("130").get("subfields")[0].get("a")
                    elif "245" in field:
                        final_hit_filtered["bn_title"] = field.get("245").get("subfields")[0].get("a")
                    elif "380" in field:
                        magic_add_to_dict("bn_form_of_work", field.get("380").get("subfields")[0].get("a"), final_hit_filtered)
                    elif "650" in field:
                        magic_add_to_dict("bn_subjects", field.get("650").get("subfields")[0].get("a"), final_hit_filtered)
                    elif "655" in field:
                        magic_add_to_dict("bn_genre_form", field.get("655").get("subfields")[0].get("a"), final_hit_filtered)
                    elif "700" in field:
                        magic_add_to_dict("bn_secondary_author", (field.get("700").get("subfields")[0].get("a"), field.get("700").get("subfields")[-1].get("e")), final_hit_filtered)

                pbl_record["BN_INFO"] = final_hit_filtered
            if pbl_record == None:
                print("No record")
            else:
                return pbl_record
    except AttributeError:
        print("No record found")
    except TypeError:
        print("No data found")
