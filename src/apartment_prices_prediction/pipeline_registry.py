"""Project pipelines."""
from __future__ import annotations

from kedro.framework.project import find_pipelines
from kedro.pipeline import Pipeline

from .pipelines.autogluon_model.pipeline import create_pipeline as create_autogluon_model_pipeline
from .pipelines.data_processing.pipeline import create_pipeline as create_data_processing_pipeline
from .pipelines.azure_upload.pipeline import create_pipeline as create_azure_upload_pipeline


def register_pipelines() -> dict[str, Pipeline]:
    pipelines = find_pipelines()

    data_processing = create_data_processing_pipeline()
    autogluon_model = create_autogluon_model_pipeline()
    azure_upload = create_azure_upload_pipeline()

    return {
        "data_processing": data_processing,
        "autogluon_model": autogluon_model,
        "azure_upload": azure_upload,
        "__default__": data_processing + autogluon_model + azure_upload
    }
