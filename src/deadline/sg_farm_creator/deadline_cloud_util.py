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
    
    Return:
        (str) ID of the created farm
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
            logger.debug(f"farm_id: {farm_id}")
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
    
    Return:
        (str) ID of the created queue
    """
    
    try:
        deadline_client = api._session.get_boto3_client("deadline")
    except:
        raise
    
    queue_id = None
    try:
        queue_response = deadline_client.create_queue(
            displayName=display_name,
            farmId=farm_id,
            jobRunAsUser=job_run_as_user,
            dryRun=dry_run
        )
        if queue_response and "queueId" in queue_response:
            queue_id = queue_response["queueId"]
            logger.debug(f"queue_id: {queue_id}")
    except:
        raise
    
    return queue_id


def create_fleet(display_name=None, farm_id=None, role_arn=None, max_worker_count=None, configuration=None):
    """
    Creates a queue for the specified farm.
    
    Args:
        display_name: (str) Queue displayName (descriptive name visible to users)
        farm_id: (str) Farm ID
        role_arn: (str) ARN of the fleet's Role
        max_worker_count: (int) Maximum worker count
        configuration: (str) Deadline Cloud fleet configuration (JSON)
    
    Return:
        (dict) Info for the created fleet
    """
    
    try:
        deadline_client = api._session.get_boto3_client("deadline")
    except:
        raise
    
    fleet_id = None
    try:
        fleet_response = deadline_client.create_fleet(
            displayName=display_name,
            farmId=farm_id,
            roleArn=role_arn,
            maxWorkerCount=max_worker_count,
            configuration=configuration
        )

        if fleet_response and "fleetId" in fleet_response:
            fleet_id = fleet_response["fleetId"]
            logger.debug(f"fleet_id: {fleet_id}")
    except:
        raise
    
    return fleet_id


def create_queue_fleet_association(farm_id=None, queue_id=None, fleet_id=None):
    """
    Associates a fleet with the specified farm's queue.
    
    Args:
        farm_id: (str) Farm ID
        queue_id: (str) Queue ID
        fleet_id: (str) Fleet ID
    
    Return:
        (dict) Info for the created fleet association
    """
    
    try:
        deadline_client = api._session.get_boto3_client("deadline")
    except:
        raise
    
    result = None
    try:
        associate_response = deadline_client.create_queue_fleet_association(
            farmId=farm_id,
            queueId=queue_id,
            fleetId=fleet_id
        )

        if associate_response and "ResponseMetadata" in associate_response:
            if "RetryAttempts" in associate_response["ResponseMetadata"] and associate_response["ResponseMetadata"]["RetryAttempts"]:
                logger.warning(f"RetryAttempts: {associate_response['ResponseMetadata']['RetryAttempts']}")
            result = associate_response["ResponseMetadata"]["HTTPStatusCode"] == 200
            logger.debug(f"result: {result}")
    except:
        raise
    
    return result


def create_role(role_name=None, assume_role_policy_document=None):
    """
    Creates a role with the specified policy document.
    
    Args:
        role_name: (str) Role display name (must be unique)
        assume_role_policy_document: (str) Role policy document (JSON)
    
    Return:
        (dict) Created role
    """
    
    try:
        iam = api._session.get_boto3_client("iam")
    except:
        raise
    
    role = None
    try:
        role_response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_role_policy_document
        )
        if role_response and "Role" in role_response:
            role = role_response["Role"]
            logger.debug(f"role: {role}")
    except:
        raise
    
    return role


def get_role(role_name=None):
    """
    Retrieves a role with the specified name.
    
    Args:
        role_name: (str) Role display name
        
    Return:
        (dict) Role info
    """
    
    try:
        iam = api._session.get_boto3_client("iam")
    except:
        raise
    
    role = None
    try:
        role_response = iam.get_role(
            RoleName=role_name
        )
        if role_response and "Role" in role_response:
            role = role_response["Role"]
            logger.debug(f"role: {role}")
    except:
        raise
    
    return role


def put_role_policy(role_name=None, policy_name=None, policy_document=None):
    """
    Creates a role with the specified policy document.
    
    Args:
        role_name: (str) Role display name (must be unique)
        assume_role_policy_document: (str) Role policy document (JSON)
    
    Return:
        (bool) True if put_role_policy call was successful
    """
    
    try:
        iam = api._session.get_boto3_client("iam")
    except:
        raise
    
    result = None
    try:
        role_policy_response = iam.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=policy_document
        )
        if role_policy_response and "ResponseMetadata" in role_policy_response:
            if "RetryAttempts" in role_policy_response["ResponseMetadata"] and role_policy_response["ResponseMetadata"]["RetryAttempts"]:
                logger.warning(f"RetryAttempts: {role_policy_response['ResponseMetadata']['RetryAttempts']}")
            result = role_policy_response["ResponseMetadata"]["HTTPStatusCode"] == 200
            logger.debug(f"result: {result}")
    except:
        raise
    
    return result


def get_caller_identity():
    """
    Retrieves the caller's identity from STS.
    
    Return:
        (dict) Caller identity information
    """
    
    try:
        sts = api._session.get_boto3_client("sts")
    except:
        raise
    
    result = None
    try:
        caller_identity_result = sts.get_caller_identity()
        if caller_identity_result and "UserId" in caller_identity_result:
            result = caller_identity_result
            logger.debug(f"result: {result}")
    except:
        raise
    
    return result

