import logging

import logutil
from deadline.client import api


# Logging to file
logutil.add_file_handler()
logger = logging.getLogger(__name__)
logger.setLevel(logutil.get_deadline_config_level())

# Logging to stdout
log_handler = logging.StreamHandler()
log_fmt = logging.Formatter(
    "%(asctime)s - [%(levelname)-7s] "
    "[%(module)s:%(funcName)s:%(lineno)d] %(message)s"
)
log_handler.setFormatter(log_fmt)
logger.addHandler(log_handler)


def create_farm(display_name=None, studio_id=None, dry_run=None):
    """
    Creates a farm for the specified studio.
    
    Args:
        display_name: (str) Farm displayName (descriptive name visible to users)
        studio_id: (str) Deadline Cloud Nimble Studio ID
        dry_run: (bool) If true, don't commit any changes
    """
    
    try:
        deadline_client = api._session.get_boto3_client("deadline")
    except:
        raise
    
    farm_id = None
    try:
        farm_response = deadline_client.create_farm(displayName=display_name, studioId=studio_id, dryRun=dry_run)
        if farm_response and "farmId" in farm_response:
            farm_id = farm_response["farmId"]
    except:
        raise
    
    return farm_id


def create_queue(display_name=None, farm_id=None, job_run_as_user=None, dry_run=None):
    """
    Creates a queue for the specified farm.
    
    Args:
        display_name: (str) Queue displayName (descriptive name visible to users)
        farm_id: (str) Farm ID
        job_run_as_user: (dict) Deadline Cloud jobRunAsUser structure (see deadline-cloud CLI)
                         {  posix: {user: string, group: string},
                            windows: {user: string, group: string, passwordArn: string} }
        dry_run: (bool) If true, don't commit any changes
    """
    
    try:
        deadline_client = api._session.get_boto3_client("deadline")
    except:
        raise
    
    queue_id = None
    try:
        queue_response = deadline_client.create_queue(displayName=display_name, farmId=farm_id, jobRunAsUser=job_run_as_user, dryRun=dry_run)
        if queue_response and "queueId" in queue_response:
            queue_id = queue_response["queueId"]
    except:
        raise
    
    return queue_id

