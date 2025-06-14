from kedro.pipeline import Pipeline, node, pipeline

from .nodes import upload_model_to_blob


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=upload_model_to_blob,
            inputs="params:model_path",
            outputs=None,
            name="upload_model_to_azure_node",
        ),
    ])
