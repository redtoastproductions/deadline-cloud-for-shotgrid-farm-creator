## Deadline Cloud ShotGrid Farm Creator

### Installation
Ensure you have Deadline Cloud Monitor set up with a root profile configured as the default Deadline Cloud profile.

The Farm Creator requires a root profile in order to create farm and queue resources.

A server environment with continuous availability requires an access key on the profile's account.

Starting from the root directory of this repository:

**1. Create a virtual environment for the Creator**

  `python -m venv creator_env`

**2. Activate the environment**

  `creator_env/Scripts/activate`

**3. Install libraries into the environment**

  * Log in to codeartifact:
  * `aws --region us-west-2 codeartifact login --tool pip --domain amazon-deadline-cloud --domain-owner 938076848303 --repository amazon-deadline-cloud-client-software`
  * Install the Deadline Cloud client library:
  * `pip install deadline`
  
  * Install the shotgun_api3 library:
  `pip install git+https://github.com/shotgunsoftware/python-api.git`

  * Install Flask:
  `pip install flask`

**4. Configure Farm Creator settings**

  * First, copy `src/deadline/sg_farm_creator/settings/settings-example.json` to `src/deadline/sg_farm_creator/settings/settings.json`
  * Change the settings as needed for your site:
  * `listener_host`: IP address to bind and listen on. You can specify any host that your users will be able to connect to from their web browser, i.e. private network addresses are acceptable if they're on that network.
  * `listener_port`: port for listening
  * `studio_id`: Your Nimble Studio ID
  * `max_worker_count`: The maximum number of workers that should run at a time on queues created by the Farm Creator.
  * `job_run_as_user`: If `runAs` is set to `CONFIGURED_QUEUE_USER`, workers will run jobs as this user and group. If `runAs` is `WORKER_AGENT_USER`, workers will run as their own user. Please see Deadline Cloud documentation for more information.
  * `job_attachment_settings`: The S3 bucket name and data root prefix for storing job attachment assets. The bucket must exist and workers must have access to the bucket.

**5. Start the Farm Creator listener**

  `python src/deadline/sg_farm_creator/listener.py`

**6. Create a Group on ShotGrid**

  If you don't have a Deadline Cloud Admin group already, you should create one now.
  
  * On your ShotGrid instance, go to the Admin menu in the top right and choose Groups
  * Click Add Group
  * Group Name: Deadline Cloud Admin
  * Users: Add anyone who needs access to Deadline Cloud administrative features: render staff, IT, TDs, producers.
  * Click "Create Group" (or "Create Group and Keep Form Values" if your instance shows that name)

**7. Configure Action Menu Items on ShotGrid**

  * On your ShotGrid instance, go to the Admin menu in the top right and choose Action Menu Items
  * Click Add Action Menu Item in the top left. Enter the following settings:
  * Title: Create Deadline Cloud Farm
  * Entity Type: Project
  * URL: `http://[listener_host]:[listener_port]/farm_creator`  (e.g. if your host is `127.0.0.1` and your port is `8090`, your URL is: `http://127.0.0.1:8090/farm_creator`)
  * Click "More Fields"
  * Enable "Restrict to Permission Groups"
  * Click the "Restrict to Permission Groups" field
  * Add "Deadline Cloud Admin"
  * Click "Create Action Menu Item" (or "Create Action Menu Item and Keep Form Values" if your instance shows that name)

**8. Use the Farm Creator**

  * On your ShotGrid instance, go to a Project page (the Project Details tab will be highlighted)
  * In the Project Actions menu in the top right, click Create Deadline Cloud Farm


### Development notes
The Farm Creator uses the Deadline Cloud log level. You can change it with:
`deadline config set settings.log_level LOG_LEVEL`

**Command line invocation**
Enclose parameters containing space characters inside double quotes, e.g. `-f "Example Farm Name"`

`python creator.py -s (studio id) -f (farm name) -q (queue name) -l (fleet name) -b (job attachments bucket name) -p (job attachments prefix)

## License

This project is licensed under the Apache-2.0 License.
