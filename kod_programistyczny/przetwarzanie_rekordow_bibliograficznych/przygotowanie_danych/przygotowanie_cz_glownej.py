# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re
import os

def readFile(path):
    main_file = pd.DataFrame(open(path, "r", encoding='utf-8').readlines(), columns=['Record'])
    return main_file

def extract_and_validate_order_numbers(df: pd.DataFrame) -> pd.DataFrame:
    df['OrderNumber'] = df['Record'].str.extract(r'^(\d{1,5})').astype(float)
    df['ValidOrder'] = True
    df['Hasło przedmiotowe'] = np.nan  # Inicjalizacja nowej kolumny

    def is_mostly_uppercase(text):
        letters = [char for char in text if char.isalpha()]
        if not letters:
            return False
        uppercase_letters = sum(1 for char in letters if char.isupper())
        return uppercase_letters / len(letters) > 0.5

    last_valid_order = None
    for index, row in df.iterrows():
        if pd.isna(row['OrderNumber']):
            if is_mostly_uppercase(row['Record']):
                df.at[index, 'Hasło przedmiotowe'] = row['Record']
            continue
        if last_valid_order is None:
            last_valid_order = row['OrderNumber']
        elif 0 < row['OrderNumber'] - last_valid_order <= 100:
            last_valid_order = row['OrderNumber']
        else:
            df.at[index, 'ValidOrder'] = False
            df.at[index, 'OrderNumber'] = np.nan

    return df

def merge_rows_based_on_order_number(df):
    df['OrderNumber'].fillna(method='ffill', inplace=True)
    record_agg = df.groupby('OrderNumber')['Record'].apply(lambda x: ' '.join(x.astype(str)))
    other_cols_agg = df.groupby('OrderNumber').agg(lambda x: x.dropna().unique().tolist())
    grouped_df = pd.DataFrame(record_agg).join(other_cols_agg.drop(columns=['Record']))
    for col in grouped_df.columns:
        grouped_df[col] = grouped_df[col].apply(lambda x: x[0] if len(x) == 1 else x)

    return grouped_df.reset_index()

def split_rows_by_patterns(df, column, patterns):
    pattern = '|'.join(patterns)

    new_rows = []
    for index, row in df.iterrows():
        parts = re.split(pattern, row[column])
        for i, part in enumerate(parts):
            new_row = {}
            for col in df.columns:
                new_row[col] = row[col] if i == 0 else np.nan
            new_row[column] = part  
            new_rows.append(new_row)
    new_df = pd.DataFrame(new_rows)
    return new_df

def process_prefixes(df: pd.DataFrame) -> pd.DataFrame:
    df['Wiersz'] = False
    df['Proza'] = False

    def check_prefix(record):
        if record.startswith('W:'):
            return ('Wiersz', record[2:].strip())
        elif record.startswith('P:'):
            return ('Proza', record[2:].strip())
        return (None, record)

    df[['Prefix', 'Record']] = df['Record'].apply(lambda x: pd.Series(check_prefix(x)))

    if 'Prefix' in df.columns:
        df['Wiersz'] = df['Prefix'] == 'Wiersz'
        df['Proza'] = df['Prefix'] == 'Proza'
        df.drop(columns=['Prefix'], inplace=True)

    return df

def process_file(path):
    df = readFile(path)
    PATTERNS = [
        "\\.\\s{0,1}[\u2014-]", "(?=;(?![^(]*\\)))", "(?=\[?\\/odp\\.?\]?)",
        "(?=\[?\\/polem\\.?\]?)", "(?=\[?nawiąz\\.?\]?)", "(?=Rec\\.?\:)",
        r"(?=rec[\.:]?\:)"
    ]

    df_validated = extract_and_validate_order_numbers(df)
    df_grouped = merge_rows_based_on_order_number(df_validated)
    df_grouped['Hasło'] = df_grouped['Hasło przedmiotowe'].shift(1)
    df_grouped['Record'] = df_grouped.apply(lambda row: row['Record'].replace(str(row['Hasło przedmiotowe']), ""), axis=1)
    df_grouped["Całostka"] = df_grouped['Record']
    df_splitted = split_rows_by_patterns(df_grouped, "Record", PATTERNS)
    df_splitted = df_splitted.drop("Hasło przedmiotowe", axis=1)
    df_splitted['OrderNumber'].fillna(method='ffill', inplace=True)
    df_splitted['Numer_całostki'] = [int(x) for x in df_splitted['OrderNumber']]
    df_splitted["Całostka"].fillna(method='ffill', inplace=True)
    df_splitted['Pierwszy'] = ~df_splitted['Numer_całostki'].duplicated()
    df_splitted['Record'] = df_splitted['Record'].str.replace(
        r'f([A-Z])|ffot|\s[f]\s|\n|\t',
        lambda m: 'fot' if m.group(0) == 'ffot' else (m.group(1) if m.group(1) else ' '),
        regex=True)
    df_splitted['Record'] = df_splitted.apply(lambda row: row['Record'].replace(f"{int(row['Numer_całostki'])}.", "", 1) if row['Record'].startswith(f"{int(row['Numer_całostki'])}.") else row['Record'], axis=1)
    df_splitted['Review'] = df_splitted['Record'].str.contains(r'\brec[\.:]?\:', case=False, regex=True)
    df_splitted['Record'] = df_splitted['Record'].str.replace(r'\brec[\.:]?\:', '', case=False, regex=True).str.strip()

    patterns_and_columns = {
        r'\[?\\/odp\\.?\]?': 'Response',
        r'\[?\\/polem\\.?\]?': 'Polemic',
        r'\[?nawiąz\\.?\]?': 'Reference',
        r'\[?sprost\\.?\]?': 'Correction',
    }

    for pattern, column in patterns_and_columns.items():
        df_splitted[column] = df_splitted['Record'].str.contains(pattern, case=False, regex=True)
        df_splitted['Record'] = df_splitted['Record'].str.replace(pattern, '', case=False, regex=True).str.strip()

    df_splitted['Record'] = df_splitted.apply(lambda row: row['Record'].replace(f"{int(row['Numer_całostki'])}.", "", 1) if row['Record'].startswith(f"{int(row['Numer_całostki'])}.") else row['Record'], axis=1)

    df_splitted = process_prefixes(df_splitted)

    return df_splitted

def process_multiple_files(directory_path):
    for filename in os.listdir(directory_path):
        if filename.endswith('.txt'):
            file_path = os.path.join(directory_path, filename)
            df_processed = process_file(file_path)
            output_csv_path = os.path.join(directory_path, filename.replace('.txt', '.csv'))
            df_processed.to_csv(output_csv_path, index=False)
            print(f'Przetworzono: {filename} i zapisano wynik do: {output_csv_path}')

process_multiple_files()
