import logging
from typing import Dict, List, Optional, Callable

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import MinMaxScaler

logger = logging.getLogger(__name__)


def concatenate_data(data: Dict[str, Callable[[], pd.DataFrame]]) -> pd.DataFrame:
    """Łączy dane z wielu partycji w jedną ramkę danych.

    Dodaje kolumny 'year' i 'month' na podstawie nazwy partycji.

    Args:
        data: Słownik partycji danych, gdzie klucze to identyfikatory,
              a wartości to funkcje ładujące dane.

    Returns:
        Połączona ramka danych.

    Raises:
        ValueError: Jeśli żadna partycja danych nie została załadowana.
    """
    all_dfs = []
    for partition_id, partition_load_func in data.items():
        try:
            logger.info(f"Ładowanie partycji: {partition_id}")
            df = partition_load_func()
            parts = partition_id.split('_')
            df['month'] = parts[-1].split('.')[0]
            df['year'] = parts[-2]
            all_dfs.append(df)
        except Exception as e:
            logger.error(f"Nie można załadować partycji {partition_id}: {e}")

    if not all_dfs:
        raise ValueError("Żadna partycja danych nie została pomyślnie załadowana.")

    concatenated_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Połączono dane. Kształt ramki: {concatenated_df.shape}")
    return concatenated_df


def impute_numerical_columns(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Uzupełnia brakujące wartości w kolumnach numerycznych.

    Domyślnie używa mediany. Stosuje specjalną logikę dla kolumny 'floorCount'.

    Args:
        df: Ramka danych do przetworzenia.
        columns: Lista kolumn do uzupełnienia. Jeśli None, brane są wszystkie numeryczne.

    Returns:
        Ramka danych z uzupełnionymi wartościami.
    """
    df_copy = df.copy()
    if columns is None:
        columns = df_copy.select_dtypes(include=['number']).columns.tolist()

    logger.info(f"Uzupełnianie brakujących wartości w kolumnach numerycznych: {columns}")

    # Standardowa imputacja medianą
    regular_num_cols = [col for col in columns if col != 'floorCount']
    if regular_num_cols:
        num_imputer = SimpleImputer(strategy='median')
        df_copy[regular_num_cols] = num_imputer.fit_transform(df_copy[regular_num_cols])

    # Specjalna logika dla 'floorCount'
    if 'floorCount' in columns:
        df_copy.loc[df_copy["floorCount"].isna() & (df_copy["type"] == "tenement"), "floorCount"] = 5
        df_copy.loc[df_copy["floorCount"].isna() & (df_copy["type"] == "blockOfFlats"), "floorCount"] = 11
        df_copy.loc[df_copy["floorCount"].isna() & (df_copy["type"] == "apartmentBuilding"), "floorCount"] = 20
        df_copy.loc[df_copy["floorCount"].isna() & (df_copy["hasElevator"] == "yes"), "floorCount"] = 9

        if df_copy["floorCount"].isna().any():
            floor_imputer = SimpleImputer(strategy='median')
            df_copy['floorCount'] = floor_imputer.fit_transform(df_copy[['floorCount']])

        # Poprawka: liczba pięter nie może być mniejsza niż piętro mieszkania
        mask_fix = df_copy["floorCount"] < df_copy["floor"]
        df_copy.loc[mask_fix, "floorCount"] = df_copy.loc[mask_fix, "floor"]

        # Usunięcie wierszy, gdzie floorCount == 1 (parterowe budynki bez pięter)
        initial_rows = len(df_copy)
        df_copy = df_copy[df_copy["floorCount"] != 1].reset_index(drop=True)
        logger.info(f"Usunięto {initial_rows - len(df_copy)} wierszy, gdzie floorCount == 1.")

    return df_copy


def impute_categorical_columns(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Uzupełnia brakujące wartości w kolumnach kategorycznych.

    Domyślnie używa dominanty (najczęściej występującej wartości).

    Args:
        df: Ramka danych do przetworzenia.
        columns: Lista kolumn do uzupełnienia. Jeśli None, brane są wszystkie kategoryczne.

    Returns:
        Ramka danych z uzupełnionymi wartościami.
    """
    df_copy = df.copy()
    if columns is None:
        columns = df_copy.select_dtypes(include=['object', 'category']).columns.tolist()

    if columns:
        logger.info(f"Uzupełnianie brakujących wartości w kolumnach kategorycznych: {columns}")
        cat_imputer = SimpleImputer(strategy='most_frequent')
        df_copy[columns] = cat_imputer.fit_transform(df_copy[columns])

    return df_copy


def remove_outliers(df: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Usuwa wartości odstające z podanych kolumn numerycznych metodą IQR.

    Args:
        df: Ramka danych do przetworzenia.
        columns: Lista kolumn, z których mają być usunięte wartości odstające.
                 Jeśli None, brane są wszystkie kolumny numeryczne.

    Returns:
        Ramka danych bez wartości odstających.
    """
    df_copy = df.copy()
    if columns is None:
        numeric_cols = df_copy.select_dtypes(include=np.number).columns.tolist()
        columns_to_exclude = ['year', 'month']
        columns = [col for col in numeric_cols if col not in columns_to_exclude]

    logger.info(f"Usuwanie wartości odstających z kolumn: {columns}")
    initial_rows = len(df_copy)

    for col in columns:
        if col in df_copy.columns:
            Q1 = df_copy[col].quantile(0.25)
            Q3 = df_copy[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df_copy = df_copy[(df_copy[col] >= lower_bound) & (df_copy[col] <= upper_bound)]

    rows_removed = initial_rows - len(df_copy)
    logger.info(f"Usunięto {rows_removed} wierszy jako wartości odstające.")
    return df_copy.reset_index(drop=True)


def normalize_numerical_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizuje wybrane kolumny numeryczne do zakresu [0, 1].

    Args:
        df: Ramka danych do przetworzenia.

    Returns:
        Ramka danych z przeskalowanymi kolumnami.
    """
    df_copy = df.copy()
    columns_to_normalize = df_copy.select_dtypes(include=np.number).columns.tolist()
    columns_to_exclude = ['buildYear', 'price', 'year', 'month', 'latitude', 'longitude', 'floor', 'floorCount',
                          'floor_ratio']
    columns_to_normalize = [col for col in columns_to_normalize if col not in columns_to_exclude]

    if not columns_to_normalize:
        logger.warning("Brak kolumn do normalizacji.")
        return df_copy

    logger.info(f"Normalizacja kolumn numerycznych: {columns_to_normalize}")
    scaler = MinMaxScaler()
    df_copy[columns_to_normalize] = scaler.fit_transform(df_copy[columns_to_normalize])
    return df_copy


def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Tworzy nowe cechy na podstawie istniejących danych.

    Tworzy cechy 'age' (wiek budynku) oraz 'floor_ratio' (stosunek piętra do liczby pięter).

    Args:
        df: Ramka danych do przetworzenia.

    Returns:
        Ramka danych z nowymi cechami.
    """
    df_copy = df.copy()
    logger.info("Rozpoczynanie inżynierii cech.")

    df_copy['year'] = pd.to_numeric(df_copy['year'], errors='coerce')
    df_copy['buildYear'] = pd.to_numeric(df_copy['buildYear'], errors='coerce')
    df_copy['age'] = df_copy['year'] - df_copy['buildYear']
    df_copy.loc[(df_copy['age'] < 0) | (df_copy['age'] > 200), 'age'] = np.nan
    logger.info("Utworzono cechę 'age'.")

    df_copy['floor_ratio'] = (df_copy['floor'] / df_copy['floorCount']).replace([np.inf, -np.inf], np.nan)
    df_copy.loc[df_copy['floorCount'] <= 0, 'floor_ratio'] = 0
    logger.info("Utworzono cechę 'floor_ratio'.")

    logger.info("Zakończono inżynierię cech.")
    return df_copy
