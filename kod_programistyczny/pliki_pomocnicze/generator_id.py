def generate_unique_id(entity_type, entity_date, bibliographic_entity_id, ordinal):
    type_codes = {
        'rekord_bibliograficzny': '0000',
        'osoba': '0001',
        'organizacja': '0002',
        'miejsce': '0003',
        'czasopismo': '0004',
        'temat': '0005'
    }
    type_code = type_codes.get(entity_type, '0000')
    return f"{entity_date}{type_code}{bibliographic_entity_id:04d}{ordinal:04d}"

def assign_ids_to_records(records, entity_type, entity_date):
    ordinal_counter = {}
    for record in records:
        bibliographic_entity_id = int(float(record['ID']))
        ordinal_counter.setdefault(bibliographic_entity_id, 0)
        ordinal_counter[bibliographic_entity_id] += 1

        unique_id = generate_unique_id(entity_type, entity_date, bibliographic_entity_id, ordinal_counter[bibliographic_entity_id])
        record['unique_id'] = unique_id

    return records

