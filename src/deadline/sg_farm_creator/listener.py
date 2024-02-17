# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from flask import Flask, redirect, request

import json
import logutil
import logging
import os
import sys
import traceback
import urllib

from creator import create_farm_and_fleet, FLEET_CONFIGURATION_PATH


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


app = Flask(__name__)
app.secret_key = __file__


CONFIGURATION_DATA_PATH = "./settings/settings.json"


def get_configuration_data(json_path):
    """
    Returns a dictionary containing configuration data.
    Data is stored as JSON.
    
    Returns None if the document cannot be parsed as JSON.
    
    Args:
        json_path: (str) Path to JSON document
    
    Return:
        (dict) Configuration settings
    """
    
    data = {}
    
    data_path = os.path.normpath(json_path)
    
    with open(data_path, "r") as f:
        try:
            data = json.loads(f.read())
        except Exception as e:
            logger.exception(e)
    
    if not data:
        data = {}
    
    return data


def _parse_url(url):
    """
    Parses a url query with key=value pairs into a dictionary.
    
    Raises exceptions on error.
    
    Args:
        url: (str) url to parse
    
    Return:
        (dict) Parameters from the query
    """
    
    if "?" not in url:
        raise ValueError("No parameters given")
    
    # Get the parameters as a list of key=value pairs 
    try:
        params_raw = urllib.parse.urlsplit(url).query.split("?")[-1].split("&")
    except:
        raise
    
    # If columns are returned, build lists of column results
    params = {"column_display_names": [], "cols": []}
    for param in params_raw:
        key, value = map(urllib.parse.unquote, param.split("=", 1))
        if key in ["column_display_names", "cols"]:
            # column_display_names and cols occur for each column
            params[key].append(value)
        else:
            params[key] = value
    
    logger.debug(f"params: {params}")
    
    return params


@app.route("/farm_creator", methods=["GET", "POST"])
def farm_creator():
    """
    ShotGrid Action Menu Item handler that creates resources on Deadline Cloud.
    
    The farm is named after the ShotGrid instance.
    The queue and fleet are named after the user's selected ShotGrid Project entity.
    
    Redirects client to the created farm on success.
    
    Returns HTML to client on error.
    
    Return:
        (str) Error messages
    """
    
    logger.debug(f"request.method: {request.method}")
    
    params = None
    if request.method == "POST":
        try:
            params = request.form.to_dict()
        except:
            return traceback.format_exc()
        
    elif request.method == "GET":
        try:
            params = _parse_url(request.url)
        except:
            return traceback.format_exc()
        
    else:
        pass
    
    if not params:
        return "Error: No parameters could be parsed."
    
    
    # Validate Action Menu Item call
    if "project_name" not in params:
        # The call was made from a page showing a non-Project entity
        return "Error: Please run this Action Menu Item from the Project Actions menu of a Project page."
    
    
    result = None
    
    # Parse params
    logger.debug(f"params: {params}")
    project_name = None
    hostname = None
    try:
        project_name = params["project_name"]
        hostname = params["server_hostname"].split(".", 1)[0]
    except:
        return traceback.format_exc()
    
    name_farm = project_name
    name_queue = f"{project_name} Queue"
    name_fleet = f"{project_name} Fleet"
    
    
    farm_result = None
    try:
        # farm_result = create_farm_and_fleet()
        farm_result = create_farm_and_fleet(
            studio_id=settings["studio_id"],
            name_farm=name_farm,
            name_queue=name_queue,
            name_fleet=name_fleet,
            fleet_configuration_path=FLEET_CONFIGURATION_PATH,
            max_worker_count=int(settings["max_worker_count"]),
            job_run_as_user=settings["job_run_as_user"],
            job_attachment_settings=settings["job_attachment_settings"]
        )
        logger.info(f"farm_result: {farm_result}")
        
    except:
        result = traceback.format_exc()
        return result
    
    # Alert user if there was an error that couldn't be directly reported
    if farm_result is None:
        result = "Error: No result was returned by the farm creator. Please check the farm creator logs for more information."
        return result
    
    
    # Alert user to reported errors
    if "error" in farm_result:
        return f"Error: {farm_result['error']}"
    
    
    try:
        region_id = settings["studio_id"].split(":")[0]
        farm_id = farm_result["farm_id"]
        farm_url = f"https://{region_id}.console.aws.amazon.com/deadlinecloud/home?region={region_id}#/farms/{farm_id}"
        
        logger.info(f"Redirecting to: {farm_url}")
        result = redirect(farm_url)
    except:
        result = traceback.format_exc()
    
    return result


if __name__ == "__main__":
    # Verify settings file exists
    if not os.path.exists(CONFIGURATION_DATA_PATH):
        logger.error(f"Path not found: {CONFIGURATION_DATA_PATH}")
        sys.exit(1)
    
    # Load settings
    try:
        settings = get_configuration_data(CONFIGURATION_DATA_PATH)
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
    
    # Validate base settings
    try:
        host = settings["listener_host"]
        port = settings["listener_port"]
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
    
    # Start listener
    app.run(host=host, port=port)
