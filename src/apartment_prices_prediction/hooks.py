import os

from dotenv import load_dotenv
from kedro.framework.hooks import hook_impl


class ProjectHooks:
    @hook_impl
    def before_pipeline_run(self):
        load_dotenv(verbose=True)
        print(
            f"Environment loaded, AZURE_STORAGE_CONNECTION_STRING exists: {'AZURE_STORAGE_CONNECTION_STRING' in os.environ}")
