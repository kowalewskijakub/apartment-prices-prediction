from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    concatenate_data,
    feature_engineering,
    impute_categorical_columns,
    impute_numerical_columns,
    normalize_numerical_columns,
    remove_outliers,
)


def create_pipeline(**kwargs) -> Pipeline:
    """Tworzy pipeline do przetwarzania surowych danych.

    Returns:
        Instancja pipeline'u Kedro.
    """
    return pipeline(
        [
            node(
                func=concatenate_data,
                inputs="apartments_partitioned",
                outputs="concatenated_apartments",
                name="concatenate_monthly_data_node",
            ),
            node(
                func=impute_numerical_columns,
                inputs="concatenated_apartments",
                outputs="intermediate_imputed_numerical",
                name="impute_numerical_columns_node",
            ),
            node(
                func=impute_categorical_columns,
                inputs="intermediate_imputed_numerical",
                outputs="intermediate_imputed_all",
                name="impute_categorical_columns_node",
            ),
            node(
                func=remove_outliers,
                inputs="intermediate_imputed_all",
                outputs="apartments_without_outliers",
                name="outlier_removal_node",
            ),
            node(
                func=feature_engineering,
                inputs="apartments_without_outliers",
                outputs="primary_apartments",
                name="feature_engineering_node",
            ),
            node(
                func=normalize_numerical_columns,
                inputs="primary_apartments",
                outputs="primary_normalized_apartments",
                name="normalization_node",
            ),
        ]
    )
