from kedro.pipeline import Pipeline, node, pipeline

from .nodes import split_data, train_autogluon_model, predict_autogluon_model


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=split_data,
            inputs={"df": "primary_normalized_apartments", "target_col": "params:ag_label"},
            outputs=["train_data", "test_data"],
            name="split_data_node",
        ),
        node(
            func=train_autogluon_model,
            inputs={"train_data": "train_data", "label": "params:ag_label"},
            outputs="trained_model",
            name="train_autogluon_model_node",
        ),
        node(
            func=predict_autogluon_model,
            inputs={"predictor": "trained_model", "test_data": "test_data"},
            outputs="predictions",
            name="predict_autogluon_model_node",
        ),
    ])
