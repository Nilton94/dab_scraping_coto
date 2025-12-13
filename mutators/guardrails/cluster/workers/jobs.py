from dataclasses import replace
from databricks.bundles.core import Bundle, job_mutator
from databricks.bundles.jobs import Job, JobCluster

@job_mutator
def enforce_dev_cluster_limits(bundle: Bundle, job: Job) -> Job:
    '''
    Mutator that enforces cluster size limits in dev environment.
    It's necessary to pass this code in databricks.yaml under mutators to have it applied.
        - mutators.guardrails.cluster.jobs:enforce_dev_cluster_limits
    
    For dev environment, this mutator:
    - Limits autoscale max_workers to 2, if autoscaling is used and removes num_workers
    - Limits fixed num_workers to 2 if higher
    - Applies to both job_clusters and task-level clusters
    
    Args:
        bundle (Bundle): The bundle containing the job.
        job (Job): The job to potentially modify.
    Returns:
        Job: The modified job with enforced cluster limits.
    '''
    
    # Only dev
    if bundle.target != "dev":
        return job
    
    MAX_WORKERS_DEV = 2
    
    # Modify job-level clusters (job_clusters)
    if job.job_clusters:
        modified_clusters = []
        for jc in job.job_clusters:
            modified_jc = _enforce_cluster_limits(jc, MAX_WORKERS_DEV)
            modified_clusters.append(modified_jc)
        job = replace(job, job_clusters=modified_clusters)
    
    # Modify task-level clusters
    if job.tasks:
        modified_tasks = []
        for task in job.tasks:
            modified_task = _enforce_task_cluster_limits(task, MAX_WORKERS_DEV)
            modified_tasks.append(modified_task)
        job = replace(job, tasks=modified_tasks)
    
    return job


def _enforce_cluster_limits(job_cluster: JobCluster, max_workers: int) -> JobCluster:
    '''
    Enforce cluster size limits on a JobCluster.
    
    Args:
        job_cluster: The job cluster to modify
        max_workers: Maximum allowed workers
    Returns:
        Modified JobCluster with enforced limits
    '''
    cluster_spec = job_cluster.new_cluster
    
    if not cluster_spec:
        return job_cluster
    
    # Handle autoscale clusters
    if cluster_spec.autoscale:
        autoscale = cluster_spec.autoscale
        
        # Enforce max_workers limit
        if autoscale.max_workers is None or autoscale.max_workers > max_workers:
            autoscale = replace(autoscale, max_workers=max_workers)
        
        # Ensure min_workers doesn't exceed max_workers
        if autoscale.min_workers and autoscale.min_workers > max_workers:
            autoscale = replace(autoscale, min_workers=1)
        
        cluster_spec = replace(cluster_spec, autoscale=autoscale)
        cluster_spec = replace(cluster_spec, num_workers=None)
    
    # Handle fixed size clusters
    elif cluster_spec.num_workers is not None and cluster_spec.num_workers > max_workers:
        cluster_spec = replace(cluster_spec, num_workers=max_workers)
    
    return replace(job_cluster, new_cluster=cluster_spec)


def _enforce_task_cluster_limits(task, max_workers: int):
    '''
    Enforce cluster size limits on task-level clusters.
    
    Args:
        task: The task to modify
        max_workers: Maximum allowed workers
    Returns:
        Modified task with enforced limits
    '''
    # Handle new_cluster at task level
    if task.new_cluster:
        cluster_spec = task.new_cluster
        
        # Handle autoscale
        if cluster_spec.autoscale:
            autoscale = cluster_spec.autoscale
            
            if autoscale.max_workers is None or autoscale.max_workers > max_workers:
                autoscale = replace(autoscale, max_workers=max_workers)
            
            if autoscale.min_workers and autoscale.min_workers > max_workers:
                autoscale = replace(autoscale, min_workers=1)
            
            cluster_spec = replace(cluster_spec, autoscale=autoscale)
            cluster_spec = replace(cluster_spec, num_workers=None)
        
        # Handle fixed size
        elif cluster_spec.num_workers is not None and cluster_spec.num_workers > max_workers:
            cluster_spec = replace(cluster_spec, num_workers=max_workers)
        
        task = replace(task, new_cluster=cluster_spec)
    
    return task