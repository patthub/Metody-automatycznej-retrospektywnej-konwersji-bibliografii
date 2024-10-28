import spacy
from spacy.tokens import DocBin
from tqdm import tqdm
import json
import pandas as pd
from spacy import displacy
from pathlib import Path
from multiprocessing import Pool


with open('/content/drive/MyDrive/Doktorat/new_model/07062024_416.json', 'r', encoding='utf-8') as file:
    data_to_transform = json.load(file)


def transform_data(data):
    transformed_data = []
    for record in data['annotations']:
        if record and isinstance(record, list) and len(record) == 2:
            text, entity_data = record
            if 'entities' in entity_data:
                entities = entity_data["entities"]
                transformed_entities = []

                for entity in entities:
                    start, end, label = entity
                    transformed_entities.append([start, end, label])

                transformed_data.append([text, {"entities": transformed_entities}])
            else:
                print(f'Pominięto rekord bez "entities": {record}')
        else:
            print(f'Pominięto nieprawidłowy rekord: {record}')
    return transformed_data

transformed_annotations = transform_data(data_to_transform)

output_file_path = '/content/07062024_416_transformed.json'
with open(output_file_path, 'w', encoding='utf-8') as file:
    json.dump(transformed_annotations, file, ensure_ascii=False, indent=4)

print(f'Transformacja zakończona. Wynik zapisano w pliku: {output_file_path}')

cv_data = json.load(open('/content/07062024_416_transformed.json','r'))

#Stworzenie plików konfiguracyjnych Spacy

#!python -m spacy init fill-config /content/drive/MyDrive/Doktorat/new_model/config/Kopia_base_config.cfg /content/drive/MyDrive/Doktorat/new_model/config/config1.cfg

# Trening
def get_spacy_doc(file, data):
  # Create a blank spaCy pipeline
  nlp = spacy.blank('pl')
  db = DocBin()
  for text, annot in tqdm(data):
    doc = nlp.make_doc(text)
    annot = annot['entities']

    ents = []
    entity_indices = []
    for start, end, label in annot:
      skip_entity = False
      for idx in range(start, end):
        if idx in entity_indices:
          skip_entity = True
          break
      if skip_entity:
        continue

      entity_indices = entity_indices + list(range(start, end))
      try:
        span = doc.char_span(start, end, label=label, alignment_mode='strict')
      except:
        continue

      if span is None:
        err_data = str([start, end]) + "    " + str(text) + "\n"
        file.write(err_data)
      else:
        ents.append(span)

    try:
      doc.ents = ents
      db.add(doc)
    except:
      pass

  return db

from sklearn.model_selection import train_test_split
train, test = train_test_split(cv_data, test_size=0.2)

len(train), len(test)
file = open('/content/drive/MyDrive/Doktorat/new_model/trained_models/train_file.txt','w')
db = get_spacy_doc(file, train)
db.to_disk('/content/drive/MyDrive/Doktorat/new_model/trained_models/train_data.spacy')
db = get_spacy_doc(file, test)
db.to_disk('/content/drive/MyDrive/Doktorat/new_model/trained_models/test_data.spacy')
file.close()

#Polecenia przekazywane w konsoli
#!python -m spacy debug data /content/drive/MyDrive/Doktorat/new_model/config/config1.cfg  --paths.train /content/drive/MyDrive/Doktorat/new_model/trained_models/train_data.spacy  --paths.dev /content/drive/MyDrive/Doktorat/new_model/trained_models/test_data.spacy --verbose
#!python -m spacy train /content/drive/MyDrive/Doktorat/new_model/config/config1.cfg  --output /content/drive/MyDrive/Doktorat/new_model/trained_models/output1  --paths.train /content/drive/MyDrive/Doktorat/new_model/trained_models/train_data.spacy  --paths.dev /content/drive/MyDrive/Doktorat/new_model/trained_models/test_data.spacy --gpu-id 0

#Zastosowanie modelu na danych tabelarycznych
nlp = spacy.load('/content/drive/MyDrive/Doktorat/new_model/trained_models/output1/model-best')

def process_text(text):
    if isinstance(text, str):
        doc = nlp(text)
        return [{ent.label_: {'text': ent.text, 'indeksy': [ent.start_char, ent.end_char]}} for ent in doc.ents]
    return []

def apply_ner_to_dataframe(df):
    df['Record'] = df['Record'].astype(str).fillna('')
    records = df.to_dict(orient='records')
    texts = [record['Record'] for record in records]
    with Pool() as pool:
        results = list(tqdm(pool.imap(process_text, texts), total=len(texts), desc="Processing NER"))
    ner_labels = set(label for result in results for entity in result for label in entity.keys())
    for record in records:
        for label in ner_labels:
            record[label] = []
    for record, entities in zip(records, results):
        for entity in entities:
            for ent_label, details in entity.items():
                record[ent_label].append(details)

    df = pd.DataFrame.from_records(records)
    return df
