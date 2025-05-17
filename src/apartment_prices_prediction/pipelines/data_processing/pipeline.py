from kedro.pipeline import Pipeline, node, pipeline
from .nodes import concatenate_data, impute_missing_values, impute_floor_count


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
            node(
                func=impute_floor_count,
                inputs="concatenated_apartments",
                outputs="floorCount_imputed_apartments",  # <- ten output!
                name="impute_floor_count_node",
            ),
        ]
    )
