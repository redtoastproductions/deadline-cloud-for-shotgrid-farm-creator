## Deadline Cloud ShotGrid Farm Creator

### Installation
Ensure you have Deadline Cloud Monitor set up with a root profile configured as the default Deadline Cloud profile.

The farm creator requires a root profile in order to create farm and queue resources.

A server environment with continuous availability requires an access key on the profile's account.

Starting from the root directory of this repository:

**1. Create a virtual environment for the creator**

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


### Development notes
The farm creator uses the Deadline Cloud log level. You can change it with:
`deadline config set settings.log_level LOG_LEVEL`

**Command line invocation**

`python creator.py -s (studio id) -f (farm name) -q (queue name) -l (fleet name)

## License

This project is licensed under the Apache-2.0 License.
