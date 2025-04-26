from kedro.pipeline import Pipeline, node, pipeline
from .nodes import concatenate_data, impute_missing_values

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
                func=impute_missing_values,
                inputs="concatenated_apartments",
                outputs="intermediate_imputed_apartments",
                name="impute_missing_values_node",
            ),
        ]
    )