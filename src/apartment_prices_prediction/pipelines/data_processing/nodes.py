import pandas as pd
from sklearn.impute import SimpleImputer
import logging

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


def impute_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Starting imputation. Initial shape: {df.shape}")
    missing_before = df.isnull().sum()
    logger.info(f"Missing values before imputation:\n{missing_before[missing_before > 0]}")

    numerical_cols = df.select_dtypes(include=['number']).columns
    if not numerical_cols.empty:
        logger.info(f"Imputing numerical columns with median: {numerical_cols.tolist()}")
        num_imputer = SimpleImputer(strategy='median')
        df[numerical_cols] = num_imputer.fit_transform(df[numerical_cols])

    categorical_cols = df.select_dtypes(include=['object', 'category']).columns
    if not categorical_cols.empty:
        logger.info(f"Imputing categorical columns with mode: {categorical_cols.tolist()}")
        cat_imputer = SimpleImputer(strategy='most_frequent')
        df[categorical_cols] = cat_imputer.fit_transform(df[categorical_cols])

    missing_after = df.isnull().sum()
    logger.info(f"Missing values after imputation:\n{missing_after[missing_after > 0]}")
    logger.info(f"Imputation complete. Final shape: {df.shape}")

    return df