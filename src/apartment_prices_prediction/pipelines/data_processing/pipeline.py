from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    concatenate_data,
    impute_numerical_columns,
    impute_categorical_columns,
    remove_outliers,
    normalize_numerical_columns,
    feature_engineering,
)


def create_pipeline(**kwargs) -> Pipeline:
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
                outputs="intermediate_imputed_only_numerical_apartments",
                name="impute_numerical_columns_node",
            ),
            node(
                func=impute_categorical_columns,
                inputs="intermediate_imputed_only_numerical_apartments",
                outputs="intermediate_imputed_apartments",
                name="impute_categorical_columns_node",
            ),
            node(
                func=remove_outliers,
                inputs="intermediate_imputed_apartments",
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
            )
        ]
    )
