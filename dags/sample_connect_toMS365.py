#=======================================================================================================
#meta 8/18/2025 [sanitized] Sample Connect DAG to MS365 resources: OneDrive and SharePoint 
# This DAG connects to Microsoft 365 resources (OneDrive and SharePoint) to check for input files.
# Provides a sample on how to connect to the same MS365 product and different file mgmt resources.
# Started from Airflow project 'anya-name', dags/anya-name-dag.py (see details at the end)


#history 8/18/2025 SAMPLE CONNECT DAG TO OneDrive and SharePoint
#      (already)Successful setup of local Airflow environment
#      w/ apache/airflow:2.9.1 (downgraded after struggling with apache/airflow:3.0.3)
#      (already) able to connect to OneDrive personal folder
#      Connect to SharePoint site

#8/19/2025 MODIFIED SAMPLE CONNECT SharePoint Site
#      Modified: connect to SPSite, retrieve site ID, not connecting to any other resources yet

## $sanit REMOVED, GROUP
#=======================================================================================================

#=======================================================================================================
# Standard library imports
import requests

# Third-party imports
from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable #, XCom


# # Project imports
# from utils.onedrive_helper import (
#     get_access_token,
#     #list_files,
#     download_file,
#     upload_file,
#     delete_file,
#     download_file_by_path,
#     #download_xlsx_by_path,
#     download_csv_by_path
# )
def get_access_token(client_id, tenant_id, username, password):
    token_url = f"https://REMOVED/{tenant_id}/oauth2/v2.0/token"
    data = {
        'client_id': client_id,
        'scope': 'https://graph.microsoft.com/.default',
        'username': username,
        'password': password,
        'grant_type': 'password',
    }
    response = requests.post(token_url, data=data)
    response.raise_for_status()
    return response.json()['access_token']

#=======================================================================================================
# Airflow Variables
CLIENT_ID: str = Variable.get("")
TENANT_ID: str = Variable.get("")
USERNAME: str = Variable.get("")
PASSWORD: str = Variable.get("")
#=======================================================================================================

#=======================================================================================================
# OneDrive Variables
CONFIG = Variable.get("anya_connect_config", deserialize_json=True)
SOURCE_FOLDER: str = CONFIG["source_folder"] #"https://domain.sharepoint.com/:f:/r/sites/GROUP/Shared%20Documents/anya_test_delete?REMOVED" 
USER_ID: str = CONFIG[""]
#=======================================================================================================
# SharePoint Variables
DOMAIN = "REMOVED"
SITE_NAME = "GROUP"
FOLDER_PATH = "Shared Documents/anya_test_delete"
#=======================================================================================================
logger = LoggingMixin().log
 
default_args = {
    'owner': 'airflow',
}

#=======================================================================================================
# Define the DAG using the @dag decorator
@dag(dag_id='anya_connect_dag', default_args=default_args)
def connect_to_MS365_resources():
    
    #=======================================================================================================
    # Task. Connect to OneDrive personal folder
    # Get # of files in OneDrive source folder
    # "source_folder": "anya_sandbox_folder/1_input"
    #======================================================================================================
    @task
    def check_input_in_onedrive(**context) -> None:
        """
        Connect to OneDrive and check files in the OneDrive source folder.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        graph_api_base = "https://graph.microsoft.com/v1.0"
        headers = {"Authorization": f"Bearer {access_token}"}

        # OneDrive VARS in CONFIG
        logger.info(f"Checking for input files in OneDrive source folder: {SOURCE_FOLDER}")
        url = f"{graph_api_base}/users/{USER_ID}/drive/root:/{SOURCE_FOLDER}:/children"
        logger.info(f"Requesting OneDrive files from: {url}")
        response = requests.get(url, headers=headers)
        logger.info(f"Response status code: {response.status_code}")
        #response.raise_for_status()
        files = response.json().get('value',[])
        csv_files = [f for f in files if f.get('file') and f['name'].lower().endswith('.csv')]
        logger.info(f"Found {len(csv_files)} CSV files in source folder {SOURCE_FOLDER}.")

    #=======================================================================================================
    # Task. Connect to our team SharePoint site
    # Get site ID
    # SITE_NAME = "GROUP"
    #======================================================================================================
    @task
    def connect_to_sharepoint_site(**context) -> None:
        """
        Connect to SharePoint site and get SharePoint site ID.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        graph_api_base = "https://graph.microsoft.com/v1.0"
        headers = {"Authorization": f"Bearer {access_token}"}

        # SharePoint VARS hardcoded above
        site_url = f"https://graph.microsoft.com/v1.0/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SharePoint site: {site_url}")
        response = requests.get(site_url, headers=headers)
        #Get the site ID for your SharePoint site:
        site_id = response.json()["id"]
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Got SharePoint site ID")


    #sequence of tasks
    check_input_in_onedrive()
    connect_to_sharepoint_site()


# Instantiate the DAG
dag = connect_to_MS365_resources()
#=======================================================================================================