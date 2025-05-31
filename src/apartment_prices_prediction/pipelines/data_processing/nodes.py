import logging

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import MinMaxScaler

logger = logging.getLogger(__name__)


def concatenate_data(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    all_dfs = []
    for partition_id, partition_load_func in data.items():
        logger.info(f"Loading partition: {partition_id}")
        try:
            df = partition_load_func()
            df['month'] = partition_id.split('_')[-1].split('.')[0]
            df['year'] = partition_id.split('_')[-2].split('.')[0]
            all_dfs.append(df)
        except Exception as e:
            logger.error(f"Could not load partition {partition_id}: {e}")
    if not all_dfs:
        raise ValueError("No data partitions were successfully loaded.")

    concatenated_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Concatenated data shape: {concatenated_df.shape}")
    logger.info(f"Columns in concatenated data: {concatenated_df.columns.tolist()}")
    return concatenated_df


def impute_numerical_columns(df: pd.DataFrame, columns=None) -> pd.DataFrame:
    df_copy = df.copy()

    if columns is None:
        columns = df_copy.select_dtypes(include=['number']).columns.tolist()

    logger.info(f"Imputing numerical columns with median: {columns}")

    regular_num_cols = [col for col in columns if col != 'floorCount']
    if regular_num_cols:
        num_imputer = SimpleImputer(strategy='median')
        df_copy[regular_num_cols] = num_imputer.fit_transform(df_copy[regular_num_cols])

    if 'floorCount' in columns:
        mask_tenement = df_copy["floorCount"].isna() & (df_copy["type"] == "tenement")
        df_copy.loc[mask_tenement, "floorCount"] = 5

        mask_block = df_copy["floorCount"].isna() & (df_copy["type"] == "blockOfFlats")
        df_copy.loc[mask_block, "floorCount"] = 11

        mask_apartment = df_copy["floorCount"].isna() & (df_copy["type"] == "apartmentBuilding")
        df_copy.loc[mask_apartment, "floorCount"] = 20

        mask_elevator = df_copy["floorCount"].isna() & (df_copy["hasElevator"] == "yes")
        df_copy.loc[mask_elevator, "floorCount"] = 9

        if df_copy["floorCount"].isna().any():
            floor_imputer = SimpleImputer(strategy='median')
            df_copy['floorCount'] = floor_imputer.fit_transform(df_copy[['floorCount']])

        mask_fix = df_copy["floorCount"] < df_copy["floor"]
        df_copy.loc[mask_fix, "floorCount"] = df_copy.loc[mask_fix, "floor"]

        before = len(df_copy)
        df_copy = df_copy[df_copy["floorCount"] != 1]
        logger.info(f"Removed {before - len(df_copy)} rows with floorCount == 1")

    return df_copy


def impute_categorical_columns(df: pd.DataFrame, columns=None) -> pd.DataFrame:
    df_copy = df.copy()

    if columns is None:
        columns = df_copy.select_dtypes(include=['object', 'category']).columns.tolist()

    if columns:
        logger.info(f"Imputing categorical columns with mode: {columns}")
        cat_imputer = SimpleImputer(strategy='most_frequent')
        df_copy[columns] = cat_imputer.fit_transform(df_copy[columns])

    return df_copy


def remove_outliers(df: pd.DataFrame, columns: list[str] = None) -> pd.DataFrame:
    df_copy = df.copy()

    if columns is None:
        columns = df_copy.select_dtypes(include=np.number).columns.tolist()
        columns = [col for col in columns if col not in ['year', 'month']]

    logger.info(f"Starting outlier removal for columns: {columns}")

    initial_rows = len(df_copy)
    outlier_mask = pd.Series(False, index=df_copy.index)

    for col in columns:
        if col in df_copy.columns:
            Q1 = df_copy[col].quantile(0.25)
            Q3 = df_copy[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outlier_mask = outlier_mask | (df_copy[col] < lower_bound) | (df_copy[col] > upper_bound)

    df_clean = df_copy[~outlier_mask]

    rows_removed = initial_rows - len(df_clean)
    logger.info(f"Removed {rows_removed} rows as outliers.")

    return df_clean


def normalize_numerical_columns(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()

    columns_to_normalize = df_copy.select_dtypes(include=np.number).columns.tolist()
    columns_to_exclude = ['buildYear', 'price', 'year', 'month', 'latitude', 'longitude', 'floor',
                          'floorCount', 'floor_ratio']
    columns_to_normalize = [col for col in columns_to_normalize if col not in columns_to_exclude]

    if not columns_to_normalize:
        logger.warning("No columns to normalize.")
        return df_copy

    logger.info(f"Normalizing numerical columns: {columns_to_normalize}")

    scaler = MinMaxScaler()
    df_copy[columns_to_normalize] = scaler.fit_transform(df_copy[columns_to_normalize])

    logger.info("Normalization complete.")

    return df_copy


def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    logger.info("Starting feature engineering.")

    df_copy['price_per_m2'] = df_copy['price'] / df_copy['squareMeters'].replace(0, np.nan)
    logger.info("Created 'price_per_m2' feature.")

    df_copy['year'] = pd.to_numeric(df_copy['year'], errors='coerce')
    df_copy['buildYear'] = pd.to_numeric(df_copy['buildYear'], errors='coerce')
    df_copy['age'] = df_copy['year'] - df_copy['buildYear']
    df_copy.loc[(df_copy['age'] < 0) | (df_copy['age'] > 200), 'age'] = np.nan
    logger.info("Created 'age' feature.")

    df_copy['floor_ratio'] = df_copy['floor'] / df_copy['floorCount']
    df_copy['floor_ratio'] = df_copy['floor_ratio'].replace([np.inf, -np.inf], np.nan)
    df_copy.loc[df_copy['floorCount'] <= 0, 'floor_ratio'] = 0
    logger.info("Created 'floor_ratio' feature.")

    logger.info("Feature engineering complete.")
    logger.info(f"Final columns: {df_copy.columns.tolist()}")

    return df_copy
