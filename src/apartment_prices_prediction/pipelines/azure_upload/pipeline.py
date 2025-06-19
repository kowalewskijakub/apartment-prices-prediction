from kedro.pipeline import Pipeline, node, pipeline

from .nodes import upload_model_to_blob


def create_pipeline(**kwargs) -> Pipeline:
    """Tworzy pipeline do wysyłania modelu do Azure Blob Storage.

    Returns:
        Instancja pipeline'u Kedro.
    """
    return pipeline(
        [
            node(
                func=upload_model_to_blob,
                inputs="trained_model",
                outputs=None,
                name="upload_model_to_azure_node",
            ),
        ]
    )