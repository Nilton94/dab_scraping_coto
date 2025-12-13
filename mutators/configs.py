from dataclasses import replace
from databricks.bundles.core import Bundle, job_mutator, pipeline_mutator
from databricks.bundles.jobs import Job, JobEmailNotifications, JobRunAs
from databricks.bundles.pipelines import Pipeline, RunAs as PipelineRunAs
import os

@job_mutator
def add_email_notifications(bundle: Bundle, job: Job) -> Job:
    '''
        Mutator that adds email notifications to a job if none are specified.
        It sets the notification to be sent to the current user on job failure.
        The bundle context is used to access the bundle context.
        
        Args:
            bundle (Bundle): The bundle containing the job.
            job (Job): The job to potentially modify.
        Returns:
            Job: The modified job with email notifications added if they were absent.
    '''
    
    if job.email_notifications:
        return job


    email_notifications = JobEmailNotifications.from_dict(
        {
            "on_failure": ["${workspace.current_user.userName}"]
        }
    )

    return replace(job, email_notifications=email_notifications)

@job_mutator
def set_job_run_as_identity(bundle: Bundle, job: Job) -> Job:
    '''
        Mutator that sets the run_as field based on whether the current user
        is a user email (contains @) or a service principal (no @).
        
        Args:
            bundle (Bundle): The bundle containing the job.
            job (Job): The job to potentially modify.
        Returns:
            Job: The modified job with appropriate run_as configuration.
    '''
    
    # If run_as is already set, don't override
    # if job.run_as:
    #     return job
    
    # Get current user from bundle context
    # current_user = bundle.workspace.current_user.user_name
    current_user = os.getenv("DATABRICKS_USER_NAME") or os.getenv("USER_EMAIL")
    
    # Check if it's an email
    if current_user:
        if "@" in current_user:
            run_as = JobRunAs(user_name=current_user)
        else:
            # It's a service principal
            run_as = JobRunAs(service_principal_name=current_user)

        return replace(job, run_as=run_as)
    
    else:
        return job
    
    

@pipeline_mutator
def set_pipeline_run_as_identity(bundle: Bundle, pipeline: Pipeline) -> Pipeline:
    '''
        Mutator that sets the run_as field based on whether the current user
        is a user email (contains @) or a service principal (no @).
        
        Args:
            bundle (Bundle): The bundle containing the pipeline.
            pipeline (Pipeline): The pipeline to potentially modify.
        Returns:
            Pipeline: The modified pipeline with appropriate run_as configuration.
    '''

    # Get current user from bundle context
    # current_user = bundle.workspace.current_user.user_name
    current_user = os.getenv("DATABRICKS_USER_NAME") or os.getenv("USER_EMAIL")
    
    # Check if it's an email
    if current_user:
        if "@" in current_user:
            run_as = PipelineRunAs(user_name=current_user)
        else:
            run_as = PipelineRunAs(service_principal_name=current_user)

        return replace(pipeline, run_as=run_as)
    else:
        return pipeline