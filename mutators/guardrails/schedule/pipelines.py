from dataclasses import replace
from databricks.bundles.core import Bundle, pipeline_mutator
from databricks.bundles.pipelines import Pipeline
import logging

logger = logging.getLogger(__name__)


@pipeline_mutator
def disable_pipeline_schedules_in_dev(bundle: Bundle, pipeline: Pipeline) -> Pipeline:
    '''
    Mutator that disables automatic pipeline triggers in dev environment.
    
    This prevents pipelines from running automatically in dev by:
    - Disabling continuous mode
    - Removing any development/production mode settings
    
    Pipelines will only run manually in dev environment.
    
    Args:
        bundle (Bundle): The bundle containing the pipeline.
        pipeline (Pipeline): The pipeline to potentially modify.
    Returns:
        Pipeline: The modified pipeline with automatic execution disabled in dev.
    '''
    
    # Only dev
    if bundle.target != "dev":
        return pipeline
    
    modifications = []
    
    # Disable continuous mode
    if pipeline.continuous is True:
        pipeline = replace(pipeline, continuous=False)
        modifications.append("continuous=False")
    
    # Set to development mode (not production)
    if pipeline.development is not True:
        pipeline = replace(pipeline, development=True)
        modifications.append("development=True")
    
    if modifications:
        logger.info(
            f"Pipeline '{pipeline.name}': Disabled automatic execution in dev environment "
            f"({', '.join(modifications)})"
        )
    
    return pipeline