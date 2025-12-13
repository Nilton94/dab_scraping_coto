from dataclasses import replace
from databricks.bundles.core import Bundle, pipeline_mutator
from databricks.bundles.pipelines import Pipeline, PipelineCluster

@pipeline_mutator
def enforce_dev_cluster_limits(bundle: Bundle, pipeline: Pipeline) -> Pipeline:
    '''
    Mutator that enforces cluster size limits in dev environment.
    
    For dev environment, this mutator:
    - Limits autoscale max_workers to 2, if autoscaling is used and removes num_workers
    - Limits fixed num_workers to 2 if higher
    
    Args:
        bundle (Bundle): The bundle containing the pipeline.
        pipeline (Pipeline): The pipeline to potentially modify.
    Returns:
        Pipeline: The modified pipeline with enforced cluster limits.
    '''
    
    # Only apply to dev environment
    if bundle.target != "dev":
        return pipeline
    
    MAX_WORKERS_DEV = 2
    
    # Modify pipeline-level clusters (use 'clusters' not 'pipeline_clusters')
    if pipeline.clusters:
        modified_clusters = []
        for cluster in pipeline.clusters:
            modified_cluster = _enforce_cluster_limits(cluster, MAX_WORKERS_DEV)
            modified_clusters.append(modified_cluster)
        pipeline = replace(pipeline, clusters=modified_clusters)
    
    return pipeline


def _enforce_cluster_limits(pipeline_cluster: PipelineCluster, max_workers: int) -> PipelineCluster:
    '''
    Enforce cluster size limits on a PipelineCluster.
    
    Args:
        pipeline_cluster: The pipeline cluster to modify
        max_workers: Maximum allowed workers
    Returns:
        Modified PipelineCluster with enforced limits
    '''
    # Handle autoscale clusters
    if pipeline_cluster.autoscale:
        autoscale = pipeline_cluster.autoscale
        
        # Enforce max_workers limit
        if autoscale.max_workers is None or autoscale.max_workers > max_workers:
            autoscale = replace(autoscale, max_workers=max_workers)
        
        # Ensure min_workers doesn't exceed max_workers
        if autoscale.min_workers and autoscale.min_workers > max_workers:
            autoscale = replace(autoscale, min_workers=1)
        
        pipeline_cluster = replace(pipeline_cluster, autoscale=autoscale)
        pipeline_cluster = replace(pipeline_cluster, num_workers=None)
    
    # Handle fixed size clusters
    elif pipeline_cluster.num_workers is not None and pipeline_cluster.num_workers > max_workers:
        pipeline_cluster = replace(pipeline_cluster, num_workers=max_workers)
    
    return pipeline_cluster