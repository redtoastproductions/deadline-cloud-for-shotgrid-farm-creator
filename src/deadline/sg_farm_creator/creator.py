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

ROLE_CHANGE_WAIT_SECONDS = 8


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
    job_run_as_user=None
):
    """
    Creates a Deadline Cloud farm and customer-managed fleet
    with the specified names.
    
    Args:
        studio_id: (str) Nimble Studio ID
        name_farm: (str) Farm display name (must be unique on the region)
        name_queue: (str) Queue display name
        name_fleet: (str) Fleet display name
        fleet_configuration_path: (str) Path to fleet configuration JSON
        max_worker_count: (int) Maximum worker count for auto scaling
        job_run_as_user: (dict) Job session RunAs user info object
    
    Return:
        (dict) Created farm
    """
    
    # Get caller identity for account ID and fleet role ARN
    fleet_role_arn = None
    caller_identity_account = None
    try:
        caller_identity = deadline_cloud_util.get_caller_identity()
        if "Account" in caller_identity:
            fleet_role_arn = f"arn:aws:iam::{caller_identity['Account']}:role/{name_farm}FleetRole"
            caller_identity_account = caller_identity["Account"]
            logger.debug(f"fleet_role_arn: {fleet_role_arn}")
    except:
        logger.debug(traceback.format_exc())
    
    if not fleet_role_arn:
        logger.error(f"Get caller identity failed")
        return None
    
    
    # Create farm with specified name
    farm_id = None
    try:
        farm_id = deadline_cloud_util.create_farm(
            display_name=name_farm,
            studio_id=studio_id,
            dry_run=True
        )
    except:
        logger.debug(traceback.format_exc())
        farm_id = "farm-a54710bfe3aa41a2800ba0f2f815f569"
        logger.debug(f"farm_id override: {farm_id}")
    
    if not farm_id:
        logger.error("Farm creation failed")
        return None
    
    logger.debug(f"farm_id: {farm_id}")
    
    
    # Create queue on farm
    queue_id = None
    try:
        queue_id = deadline_cloud_util.create_queue(
            display_name=name_queue,
            farm_id=farm_id,
            job_run_as_user=job_run_as_user,
            dry_run=False
        )
    except:
        logger.debug(traceback.format_exc())
    
    if not queue_id:
        return None
    
    logger.debug(f"queue_id: {queue_id}")
    
    # Get fleet role policy document
    pd_fleet_role_dict = None
    pd_fleet_role_path = "policy/iam_fleet_role.json"
    try:
        pd_fleet_role_dict = json.loads(get_json_document(pd_fleet_role_path))
    except:
        logger.debug(traceback.format_exc())
    
    if not pd_fleet_role_dict:
        logger.error(f"Couldn't get fleet role policy document: {pd_fleet_role_path}")
        return None
    
    
    # Update role policy document with AssumeRole conditions
    region_id = studio_id.split(":")[0]
    if "Statement" in pd_fleet_role_dict:
        for s in pd_fleet_role_dict["Statement"]:
            if s["Action"] == "sts:AssumeRole":
                s["Condition"] = {
                    "StringEquals": {"aws:SourceAccount": str(caller_identity_account)},
                    "ArnEquals": {"aws:SourceArn": f"arn:aws:deadline:{region_id}:{str(caller_identity_account)}:farm/{farm_id}"}
            }
    logger.debug(f"pd_fleet_role_dict: {pd_fleet_role_dict}")
    
    
    # Check if fleet role already exists
    role = None
    role_name_fleet = f"{name_farm}FleetRole"
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
        except:
            logger.debug(traceback.format_exc())
        
        if not role:
            logger.error(f"Fleet role creation failed")
            return None
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
        logger.error(f"Couldn't get {policy_name} policy document: {pd_worker_permissions_path}")
        return None
    
    
    # Attach WorkerPermissions to the fleet role from the policy document
    role_policy_result = None
    try:
        role_policy_result = deadline_cloud_util.put_role_policy(
            role_name=role_name_fleet,
            policy_name=policy_name,
            policy_document=pd_worker_permissions
        )
    except:
        logger.debug(traceback.format_exc())
    
    if not role_policy_result:
        logger.error(f"Fleet role creation failed")
        return None
        
        
    # Get fleet configuration document as a Python dict for input to create_fleet()
    fleet_configuration_path = "configuration/cmf_default.json"
    fleet_configuration = None
    try:
        fleet_configuration = json.loads(get_json_document(fleet_configuration_path))
    except:
        logger.debug(traceback.format_exc())
    
    if not fleet_configuration:
        logger.error(f"Couldn't get fleet configuration document: {fleet_configuration_path}")
        return None
    
    
    # Wait at least 5 seconds for the fleet role permissions change to take effect.
    # Without waiting, the sts:AssumeRole in create_fleet will fail.
    time.sleep(ROLE_CHANGE_WAIT_SECONDS)
    
    
    # Create a fleet with the user's specified configuration
    fleet_id = None
    try:
        fleet_id = deadline_cloud_util.create_fleet(
            display_name=name_fleet,
            farm_id=farm_id,
            role_arn=fleet_role_arn,
            max_worker_count=max_worker_count,
            configuration=fleet_configuration
        )
    except:
        logger.debug(traceback.format_exc())
    
    if not fleet_id:
        logger.error(f"Fleet creation failed")
        return None
    
    
    # Create a fleet-to-queue association
    associate_result = None
    try:
        associate_result = deadline_cloud_util.create_queue_fleet_association(
            farm_id=farm_id,
            queue_id=queue_id,
            fleet_id=fleet_id
        )
    except:
        logger.debug(traceback.format_exc())
    
    if not associate_result:
        logger.error(f"Fleet queue association failed")
        return None
    
    
    return True


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
        help="Fleet configuration path",
        default="configuration/cmf_default.json"
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
    
    args = parser.parse_args(sys.argv[1:])
    
    _farm_result = create_farm_and_fleet(
        studio_id=args.studio_id,
        name_farm=args.farm,
        name_queue=args.queue,
        name_fleet=args.fleet,
        fleet_configuration_path=args.fleet_configuration_path,
        max_worker_count=int(args.max_worker_count),
        job_run_as_user={"posix":{"user":args.user,"group":args.group}}
    )
    logger.info(_farm_result)
