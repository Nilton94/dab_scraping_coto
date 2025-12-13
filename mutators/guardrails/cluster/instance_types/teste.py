from dataclasses import replace
from databricks.bundles.core import Bundle, job_mutator, pipeline_mutator
from databricks.bundles.jobs import Job, JobCluster
from databricks.bundles.pipelines import Pipeline, PipelineCluster

# Cheapest instance types for dev by category (sorted by $/hr)
# General Purpose (M-series) - Best for most workloads
DEV_GENERAL_PURPOSE = [
    "m5a.large",      # $0.0465/hr - CHEAPEST general purpose
    "m5d.large",      # $0.051/hr
    "m5.large",       # $0.051/hr
    "m6i.large",      # $0.057/hr
    "m5a.xlarge",     # $0.0915/hr
]

# Compute Optimized (C-series) - For CPU-intensive workloads
DEV_COMPUTE_OPTIMIZED = [
    "c8g.medium",     # $0.041/hr - CHEAPEST compute
    "c7i.large",      # $0.057/hr
    "c8g.large",      # $0.081/hr
    "c5.xlarge",      # $0.0915/hr
]

# Memory Optimized (R-series) - For memory-intensive workloads  
DEV_MEMORY_OPTIMIZED = [
    "r5a.large",      # $0.0615/hr - CHEAPEST memory
    "r5d.large",      # $0.0675/hr
    "r5.large",       # $0.0675/hr
    "r8g.large",      # $0.112/hr
]

# Storage Optimized (I-series) - For storage-intensive workloads
DEV_STORAGE_OPTIMIZED = [
    "i3.large",       # $0.1125/hr - CHEAPEST storage
    "i3en.large",     # $0.1125/hr
    "i4g.large",      # $0.093/hr
]

# Combined whitelist for dev (all allowed cheap instances)
DEV_ALLOWED_INSTANCES = (
    DEV_GENERAL_PURPOSE + 
    DEV_COMPUTE_OPTIMIZED + 
    DEV_MEMORY_OPTIMIZED + 
    DEV_STORAGE_OPTIMIZED
)

# Default instance type (cheapest general purpose)
DEV_DEFAULT_INSTANCE = "m5a.large"  # $0.0465/hr

# Max cost per hour for dev instances
MAX_COST_PER_HOUR_DEV = 0.15  # $0.15/hr max

# Instance type pricing map (for cost calculation)
INSTANCE_PRICING = {
    "m5a.large": 0.0465,
    "m5d.large": 0.051,
    "m5.large": 0.051,
    "m6i.large": 0.057,
    "m5a.xlarge": 0.0915,
    "c8g.medium": 0.041,
    "c7i.large": 0.057,
    "c8g.large": 0.081,
    "c5.xlarge": 0.0915,
    "r5a.large": 0.0615,
    "r5d.large": 0.0675,
    "r5.large": 0.0675,
    "r8g.large": 0.112,
    "i3.large": 0.1125,
    "i3en.large": 0.1125,
    "i4g.large": 0.093,
}


def get_instance_cost(instance_type: str) -> float:
    """Get the cost per hour for an instance type."""
    return INSTANCE_PRICING.get(instance_type, 999.0)  # Unknown = expensive


def get_recommended_instance(current_instance: str) -> str:
    """
    Get recommended cheap instance based on current instance type.
    
    Maps expensive instances to similar but cheaper alternatives.
    """
    # Extract instance family (m5, c5, r5, etc.)
    if not current_instance:
        return DEV_DEFAULT_INSTANCE
    
    family = current_instance.split('.')[0] if '.' in current_instance else ""
    
    # Map to cheapest instance in same family
    if family.startswith('m'):  # General purpose
        return "m5a.large"  # Cheapest general purpose
    elif family.startswith('c'):  # Compute optimized
        return "c8g.medium"  # Cheapest compute
    elif family.startswith('r'):  # Memory optimized
        return "r5a.large"  # Cheapest memory
    elif family.startswith('i'):  # Storage optimized
        return "i4g.large"  # Cheapest storage
    elif family.startswith('g') or family.startswith('p'):  # GPU instances
        print(f"⚠️  GPU instances ({current_instance}) not allowed in dev - using {DEV_DEFAULT_INSTANCE}")
        return DEV_DEFAULT_INSTANCE
    else:
        return DEV_DEFAULT_INSTANCE


@job_mutator
def enforce_dev_instance_types(bundle: Bundle, job: Job) -> Job:
    '''
    Enforce cheap instance types in dev environment.
    
    This mutator:
    - Blocks expensive instances (>$0.15/hr)
    - Replaces with cheapest alternative in same family
    - Shows cost savings
    '''
    
    if bundle.target != "dev":
        return job
    
    if job.job_clusters:
        modified_clusters = []
        for jc in job.job_clusters:
            if jc.new_cluster and jc.new_cluster.node_type_id:
                current_instance = jc.new_cluster.node_type_id
                current_cost = get_instance_cost(current_instance)
                
                # Check if instance is too expensive or not in whitelist
                if current_instance not in DEV_ALLOWED_INSTANCES or current_cost > MAX_COST_PER_HOUR_DEV:
                    recommended = get_recommended_instance(current_instance)
                    recommended_cost = get_instance_cost(recommended)
                    savings_pct = ((current_cost - recommended_cost) / current_cost * 100) if current_cost > 0 else 0
                    
                    print(
                        f"💰 [INSTANCE] Job '{job.name}' cluster '{jc.job_cluster_key}':\n"
                        f"   Replaced: {current_instance} (${current_cost:.4f}/hr)\n"
                        f"   With:     {recommended} (${recommended_cost:.4f}/hr)\n"
                        f"   Savings:  {savings_pct:.1f}% per hour"
                    )
                    
                    new_cluster = replace(jc.new_cluster, node_type_id=recommended)
                    jc = replace(jc, new_cluster=new_cluster)
            
            modified_clusters.append(jc)
        job = replace(job, job_clusters=modified_clusters)
    
    return job


@pipeline_mutator
def enforce_dev_pipeline_instance_types(bundle: Bundle, pipeline: Pipeline) -> Pipeline:
    '''Enforce cheap instance types for pipelines in dev.'''
    
    if bundle.target != "dev":
        return pipeline
    
    if pipeline.clusters:
        modified_clusters = []
        for cluster in pipeline.clusters:
            if cluster.node_type_id:
                current_instance = cluster.node_type_id
                current_cost = get_instance_cost(current_instance)
                
                if current_instance not in DEV_ALLOWED_INSTANCES or current_cost > MAX_COST_PER_HOUR_DEV:
                    recommended = get_recommended_instance(current_instance)
                    recommended_cost = get_instance_cost(recommended)
                    savings_pct = ((current_cost - recommended_cost) / current_cost * 100) if current_cost > 0 else 0
                    
                    print(
                        f"💰 [INSTANCE] Pipeline '{pipeline.name}':\n"
                        f"   Replaced: {current_instance} (${current_cost:.4f}/hr)\n"
                        f"   With:     {recommended} (${recommended_cost:.4f}/hr)\n"
                        f"   Savings:  {savings_pct:.1f}% per hour"
                    )
                    
                    cluster = replace(cluster, node_type_id=recommended)
            
            modified_clusters.append(cluster)
        pipeline = replace(pipeline, clusters=modified_clusters)
    
    return pipeline