from dataclasses import replace
from databricks.bundles.core import Bundle, job_mutator
from databricks.bundles.jobs import Job
import logging

logger = logging.getLogger(__name__)


@job_mutator
def disable_job_schedules_in_dev(bundle: Bundle, job: Job) -> Job:
    '''
    Mutator that disables automatic job triggers in dev environment.
    
    This prevents jobs from running automatically in dev by:
    - Removing schedule/trigger configurations
    - Removing continuous configurations
    - Disabling queue settings
    - Setting max_concurrent_runs to 1
    
    Jobs will only run manually in dev environment.
    
    Args:
        bundle (Bundle): The bundle containing the job.
        job (Job): The job to potentially modify.
    Returns:
        Job: The modified job with schedules disabled in dev.
    '''
    
    # Only dev
    if bundle.target != "dev":
        return job
    
    modifications = []
    
    # Remove schedule
    if job.schedule:
        job = replace(job, schedule=None)
        modifications.append("schedule")
    
    # Remove trigger
    if job.trigger:
        job = replace(job, trigger=None)
        modifications.append("trigger")
    
    # Disable continuous execution
    if job.continuous:
        job = replace(job, continuous=None)
        modifications.append("continuous")
    
    # Remove queue settings (prevents job queuing)
    if job.queue:
        job = replace(job, queue=None)
        modifications.append("queue")
    
    # Limit concurrent runs to 1 in dev
    if job.max_concurrent_runs is None or job.max_concurrent_runs > 1:
        job = replace(job, max_concurrent_runs=1)
        modifications.append("max_concurrent_runs=1")
    
    if modifications:
        logger.info(
            f"Job '{job.name}': Disabled automatic triggers in dev environment "
            f"(removed: {', '.join(modifications)})"
        )
    
    return job