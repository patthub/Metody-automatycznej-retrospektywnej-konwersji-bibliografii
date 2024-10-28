import pandas as pd
import sqlite3
import re
import numpy as np
import xml.etree.ElementTree as ET
import json
from collections import Counter
import pprint

# KONFIGURACJA USTAWIEŃ DLA PANDA
pd.set_option('display.max_rows', None)

# Definicja funkcji eksplodującej wartości listowe na poszczególne wiersze
def explode(df, lst_cols, fill_value='', preserve_index=False):
    if lst_cols and not isinstance(lst_cols, (list, tuple, np.ndarray, pd.Series)):
        lst_cols = [lst_cols]
    
    idx_cols = df.columns.difference(lst_cols)
    lens = df[lst_cols[0]].str.len()
    idx = np.repeat(df.index.values, lens)
    
    res = pd.DataFrame({
                col: np.repeat(df[col].values, lens)
                for col in idx_cols},
                index=idx).assign(
                **{col: np.concatenate(df.loc[lens > 0, col].values) for col in lst_cols}
             )
    
    if (lens == 0).any():
        res = pd.concat([res, df.loc[lens == 0, idx_cols]], sort=False).fillna(fill_value)
    
    res = res.sort_index()
    if not preserve_index:        
        res = res.reset_index(drop=True)
    return res

# Ładowanie danych
try:
    df = pd.read_csv("Mickiewicz_korpus_raw.csv")
except FileNotFoundError:
    print("Plik Mickiewicz_korpus_raw.csv nie został znaleziony.")
    raise

# Czyszczenie danych
# Usunięcie pustych wartości w kolumnie "x"
df["x"].replace('', np.nan, inplace=True)
df.dropna(subset=["x"], inplace=True)

# Dodanie kolumny oznaczającej rekordy z numerami
pattern = "^\d{1,4}\."  # Zaktualizowany regex do uwzględnienia kropki po numerze

df['digit'] = df["x"].str.contains(pattern, regex=True)
df['row_id'] = np.arange(len(df))

# Funkcja pomocnicza do przypisania identyfikatora
def f(row):
    if row['digit']:
        return row['row_id']
    return np.nan

df['id'] = df.apply(lambda x: f(x), axis=1)
df['id'].ffill(axis=0, inplace=True)
df = df.groupby('id')['x'].apply(' '.join).reset_index()
df.rename(columns={"x": "Nazwa"}, inplace=True)

# Czyszczenie danych - naprawa błędów związanych z parsowaniem nawiasów kwadratowych
# np. [x-y]
df["numerCalostki"] = df["Nazwa"].str.extract('(^\d{1,4}\.)', expand=True)
df['numerCalostki'].ffill(inplace=True)
df["nawiasKw"] = df["Nazwa"].str.extract("(\[[^\]]*;.*\])", expand=True)
df["nawiasKw1"] = df["nawiasKw"].str.replace(";", "~")

df['Nazwa'] = df.apply(lambda row: row['Nazwa'].replace(str(row['nawiasKw']), str(row['nawiasKw1'])) if pd.notna(row['nawiasKw']) else row['Nazwa'], axis=1)
df['Nazwa'] = df['Nazwa'].str.replace("W;", "W:")

# Eksport do pliku CSV
df.to_csv("korpus_main.csv", index=False)

# Czyszczenie dodatkowe - dodanie kolumn, usunięcie niepotrzebnych znaków
df["tworca"] = df["Nazwa"].str.extract("([A-ZŚŁŻŹĆŃĘÓĄ1ÉŁ -]{3,100} [A-ZŚŁŻŹĆŃĘÓĄÉŁ ]{3,100})", expand=True)
df['tworca'].ffill(inplace=True)
df['Nazwa'] = df.apply(lambda row: row['Nazwa'].replace(str(row['tworca']), '') if pd.notna(row['tworca']) else row['Nazwa'], axis=1)

# Przetwarzanie skrótów
skroty = pd.read_csv("skroty.csv")
skroty_list = skroty['nazwa'].values.tolist()
skroty_pattern = '|'.join(map(re.escape, skroty_list))
df['skroty'] = df['Nazwa'].str.extract(f'({skroty_pattern})', expand=True)
df['skroty'] = df['skroty'].str[1:]
df['Nazwa'] = df.apply(lambda row: row['Nazwa'].replace(str(row['skroty']), '') if pd.notna(row['skroty']) else row['Nazwa'], axis=1)

# Ekstrakcja dodatkowych danych
# Strony, serie, nlb, tabl itd.
df["numer"] = df["Nazwa"].str.findall('nr\s\d+[\/\-]?\d*\.?')
df["strony_seria"] = df["Nazwa"].str.findall("ss\s?\.\s?[\d+\s,nlb tabl\. ilustr\. portr\. plany\. mapa\. mapy\.]*")
df["strony"] = df["Nazwa"].str.findall("s+\.\s?\d*\s?[-—]?\s?\d+\.?")
df['strony_string'] = df['strony'].apply(lambda x: ', '.join(map(str, x)))
df["strony_wszystkie"] = df["Nazwa"].str.findall("s+\.\s?\d*\s?[-—]?\s?\d+.+\.?")
df["nlb"] = df["Nazwa"].str.findall(",\s?\d+\s?nlb\.")
df['nlb_string'] = df['nlb'].apply(lambda x: ', '.join(map(str, x)))
df["tabl"] = df["Nazwa"].str.findall(",\s?tabl\.\s?\d+\.?")
df['tabl_string'] = df['tabl'].apply(lambda x: ', '.join(map(str, x)))
df["strony_wszystkie_string"] = df["strony_string"] + df["nlb_string"] + df["tabl_string"]
df["bez_stron"] = df.apply(lambda row: row['Nazwa'].replace(str(row['strony_wszystkie_string']), '') if pd.notna(row['strony_wszystkie_string']) else row['Nazwa'], axis=1)


df["adnotacja"] = df["Nazwa"].str.extract('(\[[A-ZĄĆĘŁÓŚŻŹŃa-zćęłóśżźąń\.\s\d;,]+\][\.,]?$)', expand=True)
df.to_csv("df.csv", index=False)

print("Skończono przetwarzanie danych.")

