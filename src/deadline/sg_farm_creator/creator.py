# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import argparse
import json
import logging
import sys
import time
import traceback

import deadline_cloud_util
import logutil


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


FLEET_CONFIGURATION_PATH = "configuration/cmf_default.json"
ROLE_CHANGE_WAIT_SECONDS = 6


def get_json_document(json_path=None):
    """
    Validates the specified path contains a JSON document
    and returns the document as a string.
    
    Returns None if the document cannot be parsed as JSON.
    
    Args:
        json_path: (str) Path to JSON document
    
    Return:
        (str) Document contents
    """
    
    contents_raw = None
    try:
        with open(json_path, "r") as f:
            contents_raw = f.read()
    except:
        logger.debug(traceback.format_exc())
    
    if not contents_raw:
        logger.error(f"Couldn't read policy document: {json_path}")
        return None
    
    # Ensure valid JSON
    contents_raw_json = None
    try:
        contents_raw_json = json.loads(contents_raw)
    except:
        logger.debug(traceback.format_exc())
    
    if not contents_raw_json:
        logger.error(f"Couldn't interpret policy document as JSON: {json_path}")
        return None
    
    return contents_raw


def create_farm_and_fleet(
    studio_id=None,
    name_farm=None,
    name_queue=None,
    name_fleet=None,
    fleet_configuration_path=None,
    max_worker_count=None,
    job_run_as_user=None,
    job_attachment_settings=None
):
    """
    Creates a Deadline Cloud farm and customer-managed fleet
    with the specified names.
    
    Args:
        studio_id: (str) Nimble Studio ID
        name_farm: (str) (required) Farm display name (must be unique on the region)
        name_queue: (str) (required) Queue display name
        name_fleet: (str) (required) Fleet display name
        fleet_configuration_path: (str) Path to fleet configuration JSON
        max_worker_count: (int) Maximum worker count for auto scaling
        job_run_as_user: (dict) Job session RunAs user info object
        job_attachment_settings: (dict) Job attachment settings for created queue
    
    Return:
        (dict) Info on created resources, or an error
    """
    
    # Validate input
    if name_farm is None:
        error_msg = "No farm name specified"
        logger.error(error_msg)
        return {"error": error_msg}
    
    if name_queue is None:
        error_msg = "No queue name specified"
        logger.error(error_msg)
        return {"error": error_msg}
    
    if name_fleet is None:
        error_msg = "No fleet name specified"
        logger.error(error_msg)
        return {"error": error_msg}
    
    
    # Replace spaces with underscores to build the role name
    name_farm_sanitized = name_farm.replace(" ", "_")
    role_name_fleet = f"{name_farm_sanitized}FleetRole"
    
    # Get caller identity for account ID and fleet role ARN
    fleet_role_arn = None
    try:
        caller_identity = deadline_cloud_util.get_caller_identity()
        if "Account" in caller_identity:
            fleet_role_arn = f"arn:aws:iam::{caller_identity['Account']}:role/{role_name_fleet}"
            logger.debug(f"fleet_role_arn: {fleet_role_arn}")
        else:
            logger.error(f"Get caller identity didn't return Account")
    except:
        logger.debug(traceback.format_exc())
    
    if not fleet_role_arn:
        error_msg = f"Get caller identity failed"
        logger.error(error_msg)
        return {"error": error_msg}
    
    
    # Track progress for completion and clean_up
    progress = {}
    
    
    # Create farm with specified name
    farm_id = None
    try:
        farm_id = deadline_cloud_util.create_farm(
            display_name=name_farm,
            studio_id=studio_id
        )
        progress["farm_id"] = farm_id
    except:
        error_msg = traceback.format_exc()
        logger.error(error_msg)
        return {"error": error_msg}
    
    
    if not farm_id:
        error_msg = "Farm creation failed"
        logger.error(error_msg)
        return {"error": error_msg}
    
    logger.debug(f"farm_id: {farm_id}")
    
    
    # Create queue on farm
    queue_id = None
    try:
        queue_id = deadline_cloud_util.create_queue(
            display_name=name_queue,
            farm_id=farm_id,
            job_run_as_user=job_run_as_user,
            job_attachment_settings=job_attachment_settings
        )
    except:
        logger.debug(traceback.format_exc())
    
    if not queue_id:
        error_msg = "Queue creation failed"
        logger.error(error_msg)
        clean_up(progress)
        return {"error": error_msg}
    
    logger.debug(f"queue_id: {queue_id}")
    
    
    # Check that queue is available for interaction
    queue_retries_total = 3
    queue_retries = 0
    while queue_retries < queue_retries_total:
        queue_result = None
        try:
            queue_result = deadline_cloud_util.get_queue(
                farm_id=farm_id,
                queue_id=queue_id
            )
        except:
            logger.debug(traceback.format_exc())
        
        if queue_result:
            progress["queue_id"] = queue_result
            break
        else:
            logger.warning("get_queue didn't return queueId")
        
        queue_retries += 1
    
    if not progress["queue_id"]:
        error_msg = f"Couldn't get created queue after {queue_retries} retries"
        logger.error(error_msg)
        clean_up(progress)
        return {"error": error_msg}
    
    
    # Get fleet role policy document
    pd_fleet_role_dict = None
    pd_fleet_role_path = "policy/iam_fleet_role.json"
    try:
        pd_fleet_role_dict = json.loads(get_json_document(pd_fleet_role_path))
    except:
        logger.debug(traceback.format_exc())
    
    if not pd_fleet_role_dict:
        error_msg = f"Couldn't get fleet role policy document: {pd_fleet_role_path}"
        logger.error(error_msg)
        clean_up(progress)
        return {"error": error_msg}
    
    
    # Update role policy document with AssumeRole conditions
    region_id = studio_id.split(":")[0]
    if "Statement" in pd_fleet_role_dict and pd_fleet_role_dict["Statement"]:
        for s in pd_fleet_role_dict["Statement"]:
            try:
                if s["Action"] == "sts:AssumeRole":
                    s["Condition"] = {
                        "StringEquals": {"aws:SourceAccount": str(caller_identity['Account'])},
                        "ArnEquals": {"aws:SourceArn": f"arn:aws:deadline:{region_id}:{str(caller_identity['Account'])}:farm/{farm_id}"}
                }
            except:
                error_msg = f"Couldn't set sts:AssumeRole condition"
                logger.error(error_msg)
                clean_up(progress)
                return {"error": error_msg}
    else:
        error_msg = "Statement not found in fleet role dictionary"
        logger.error(error_msg)
        clean_up(progress)
        return {"error": error_msg}
    
    logger.debug(f"pd_fleet_role_dict: {pd_fleet_role_dict}")
    
    
    # Check if fleet role already exists
    role = None
    try:
        role = deadline_cloud_util.get_role(role_name=role_name_fleet)
    except Exception as e:
        # Ignore if not found
        if e.__class__.__name__ == "NoSuchEntityException":
            pass
        else:
            logger.debug(traceback.format_exc())
    
    if role:
        logger.warning(f"Fleet role already exists: {role_name_fleet}")
    else:
        # Create fleet role with policy document
        try:
            role = deadline_cloud_util.create_role(
                role_name=role_name_fleet,
                assume_role_policy_document=json.dumps(pd_fleet_role_dict)
            )
            progress["role_name"] = role_name_fleet
        except:
            logger.debug(traceback.format_exc())
        
        if not role:
            error_msg = f"Fleet role creation failed"
            logger.error(error_msg)
            clean_up(progress)
            return {"error": error_msg}
        else:
            pass
    
    
    # Get WorkerPermissions policy document
    policy_name = "WorkerPermissions"
    pd_worker_permissions_path = "policy/iam_fleet_worker_permissions.json"
    pd_worker_permissions = None
    try:
        pd_worker_permissions = get_json_document(pd_worker_permissions_path)
    except:
        logger.debug(traceback.format_exc())
    
    if not pd_worker_permissions:
        error_msg = f"Couldn't get {policy_name} policy document: {pd_worker_permissions_path}"
        logger.error(error_msg)
        clean_up(progress)
        return {"error": error_msg}
    
    
    # Attach WorkerPermissions to the fleet role from the policy document
    role_policy = None
    try:
        role_policy = deadline_cloud_util.put_role_policy(
            role_name=role_name_fleet,
            policy_name=policy_name,
            policy_document=pd_worker_permissions
        )
        progress["role_policy"] = {
            "role_name": role_name_fleet,
            "policy_name": policy_name
        }
    except:
        logger.debug(traceback.format_exc())
    
    if not role_policy:
        error_msg = f"Fleet role creation failed"
        logger.error(error_msg)
        clean_up(progress)
        return {"error": error_msg}
        
        
    # Get fleet configuration document as a Python dict for input to create_fleet()
    fleet_configuration = None
    try:
        fleet_configuration = json.loads(get_json_document(fleet_configuration_path))
    except:
        logger.debug(traceback.format_exc())
    
    if not fleet_configuration:
        error_msg = f"Couldn't get fleet configuration document: {fleet_configuration_path}"
        logger.error(error_msg)
        clean_up(progress)
        return {"error": error_msg}
    
    
    # Create a fleet with the user's specified configuration
    role_change_retries_total = 3
    role_change_retries = 0
    while role_change_retries < role_change_retries_total:
        # Wait for the fleet role permissions change to take effect.
        # Without waiting, the sts:AssumeRole in create_fleet will fail.
        time.sleep(ROLE_CHANGE_WAIT_SECONDS)

        # Create the fleet
        fleet_id = None
        try:
            fleet_id = deadline_cloud_util.create_fleet(
                display_name=name_fleet,
                farm_id=farm_id,
                role_arn=fleet_role_arn,
                max_worker_count=max_worker_count,
                configuration=fleet_configuration
            )
            logger.debug(f"fleet_id: {fleet_id}")
        except:
            logger.debug(traceback.format_exc())
    
        if fleet_id:
            break
        
        role_change_retries += 1
    
    if not fleet_id:
        error_msg = f"Fleet creation failed after {role_change_retries} retries. Please check fleet role: {fleet_role_arn}"
        logger.error(error_msg)
        clean_up(progress)
        return {"error": error_msg}
    
    
    # Create a queue-fleet association
    associate_result = None
    logger.debug("create_queue_fleet_association")
    try:
        associate_result = deadline_cloud_util.create_queue_fleet_association(
            farm_id=farm_id,
            queue_id=queue_id,
            fleet_id=fleet_id
        )
        logger.debug(f"associate_result: {associate_result}")
    except:
        logger.debug(traceback.format_exc())
    
    if not associate_result:
        error_msg = f"Fleet queue association failed"
        logger.error(error_msg)
        clean_up(progress)
        return {"error": error_msg}
    
    
    return progress


def clean_up(resources=None):
    """
    Removes resources created during a farm creation run.
    
    Returns a key-for-key dictionary of results for each removal.
    
    Args:
        resources: (dict) AWS resources to remove. _farm_id will not be removed.
    
    Return:
        (dict) Results of each removal
    """
    
    cleaned = {}
    
    logger.debug(f"resources: {resources}")
    
    if "role_policy" in resources:
        logger.debug(f"delete role_policy {resources['role_policy']}")
        try:
            cleaned["delete_role_policy"] = deadline_cloud_util.delete_role_policy(
                role_name=resources["role_policy"]["role_name"],
                policy_name=resources["role_policy"]["policy_name"]
            )
        except:
            logger.debug(traceback.format_exc())
    
    if "role_name" in resources:
        try:
            cleaned["role_name"] = deadline_cloud_util.delete_role(role_name=resources["role_name"])
        except:
            logger.debug(traceback.format_exc())

    if "queue_id" in resources:
        try:
            cleaned["delete_queue"] = deadline_cloud_util.delete_queue(farm_id=resources["farm_id"], queue_id=resources["queue_id"])
        except:
            logger.debug(traceback.format_exc())
    
    if "farm_id" in resources:
        try:
            cleaned["delete_farm"] = deadline_cloud_util.delete_farm(farm_id=resources["farm_id"])
        except:
            logger.debug(traceback.format_exc())

    logger.debug(f"cleaned: {cleaned}")
    
    return cleaned


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a farm, queue, and fleet in a studio."
    )
    parser.add_argument(
        "-s", "--studio_id",
        help="Nimble Studio ID"
    )
    parser.add_argument(
        "-f", "--farm",
        help="Farm name"
    )
    parser.add_argument(
        "-q", "--queue",
        help="Queue name"
    )
    parser.add_argument(
        "-l", "--fleet",
        help="Fleet name"
    )
    parser.add_argument(
        "-fcp", "--fleet_configuration_path",
        help="Fleet configuration path"
    )
    parser.add_argument(
        "-m", "--max_worker_count",
        help="Maximum worker count for autoscaling",
        default=1
    )
    parser.add_argument(
        "-u", "--user",
        help="Posix job RunAs user name",
        default="jobuser"
    )
    parser.add_argument(
        "-g", "--group",
        help="Posix job RunAs group name",
        default="jobgroup"
    )
    parser.add_argument(
        "-b", "--job_attachment_bucket",
        help="S3 job attachment bucket name"
    )
    parser.add_argument(
        "-p", "--root_prefix",
        help="S3 job attachment root prefix"
    )
    parser.add_argument(
        "-a", "--run_as",
        help="Job runAs",
        choices=["WORKER_AGENT_USER", "CONFIGURED_QUEUE_USER"],
        default="WORKER_AGENT_USER"
    )
    
    args = parser.parse_args(sys.argv[1:])
    
    fleet_configuration_path = None
    if not args.fleet_configuration_path:
        fleet_configuration_path = FLEET_CONFIGURATION_PATH
    
    _farm_result = create_farm_and_fleet(
        studio_id=args.studio_id,
        name_farm=args.farm,
        name_queue=args.queue,
        name_fleet=args.fleet,
        fleet_configuration_path=fleet_configuration_path,
        max_worker_count=int(args.max_worker_count),
        job_run_as_user={"posix":{"user":args.user,"group":args.group}, "runAs": args.run_as},
        job_attachment_settings={"s3BucketName": args.job_attachment_bucket, "rootPrefix": args.root_prefix}
    )
    logger.info(_farm_result)
