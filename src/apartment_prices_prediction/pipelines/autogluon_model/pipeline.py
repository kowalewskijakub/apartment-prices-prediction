from kedro.pipeline import Pipeline, node, pipeline

from .nodes import predict_with_autogluon, split_data, train_autogluon_model


def create_pipeline(**kwargs) -> Pipeline:
    """Tworzy pipeline do trenowania i oceny modelu AutoGluon.

    Returns:
        Instancja pipeline'u Kedro.
    """
    return pipeline(
        [
            node(
                func=split_data,
                inputs={"df": "primary_normalized_apartments", "target_col": "params:ag_label"},
                outputs=["train_data", "test_data"],
                name="split_data_node",
            ),
            node(
                func=train_autogluon_model,
                inputs={
                    "train_data": "train_data",
                    "label": "params:ag_label",
                    "excluded_features": "params:ag_excluded_features",
                    "model_path": "params:model_path",
                },
                outputs="trained_model",
                name="train_autogluon_model_node",
            ),
            node(
                func=predict_with_autogluon,
                inputs={"predictor_path": "trained_model", "test_data": "test_data"},
                outputs="predictions",
                name="predict_with_autogluon_node",
            ),
        ]
    )