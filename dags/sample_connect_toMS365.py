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
#8/19/2025 ADDED CONNECT TO SharePoint SITE -> DRIVE -> FOLDER
#      Added: connecting to SharePoint drive and folder

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
GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"
#=======================================================================================================
# OneDrive Variables
CONFIG = Variable.get("anya_connect_config", deserialize_json=True)
SOURCE_FOLDER: str = CONFIG["source_folder"] #"https://domain.sharepoint.com/:f:/r/sites/GROUP/Shared%20Documents/anya_test_delete?REMOVED" 
USER_ID: str = CONFIG[""]
#=======================================================================================================
# SharePoint Variables
DOMAIN = "REMOVED"
SITE_NAME = "GROUP"
DRIVE_NAME = "Documents"
FOLDER_PATH = "anya_test_delete"
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
        headers = {"Authorization": f"Bearer {access_token}"}

        # OneDrive VARS in CONFIG
        logger.info(f"Checking for input files in OneDrive source folder: {SOURCE_FOLDER}")
        url = f"{GRAPH_API_BASE}/users/{USER_ID}/drive/root:/{SOURCE_FOLDER}:/children"
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
        headers = {"Authorization": f"Bearer {access_token}"}

        # SharePoint VARS hardcoded above
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SharePoint site: {site_url}")
        response = requests.get(site_url, headers=headers)
        #Get the site ID for your SharePoint site:
        site_id = response.json()["id"]
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Got SharePoint site ID")

    #=======================================================================================================
    # Task. Connect to team SharePoint site and list Document Libraries
    # Get site ID
    # use endpoints like: 
        # /sites/{site-id}/lists — List all SharePoint lists 
        # /sites/{site-id}/drives — List all document libraries 
        # /sites/{site-id}/users — List all users with access 
        # /sites/{site-id}/columns — List all site columns 
        # /sites/{site-id}/contentTypes — List all content types 
    #======================================================================================================
    @task
    def list_document_libraries(**context) -> None:
        """
        List all document libraries in a SharePoint site using Microsoft Graph API.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}

        # Get site ID
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SharePoint site: {site_url}")
        response = requests.get(site_url, headers=headers)
        response.raise_for_status()
        site_id = response.json()["id"]
        logger.info(f"Site ID: {site_id}")

        # List all document libraries (drives)
        url = f"{GRAPH_API_BASE}/sites/{site_id}/drives"
        logger.info(f"Requesting document libraries from site {SITE_NAME}: {url}")
        response = requests.get(url, headers=headers)
        #logger.info(f"Response status code: {response.status_code}")
        response.raise_for_status()
        drives = response.json().get('value', [])
        logger.info(f"Document libraries found: {len(drives)}")
        for drive in drives:
            logger.info(f"Library: {drive.get('name')} | ID: {drive.get('id')}")
    
    #=======================================================================================================
    # Task. Connect to team SharePoint site and list items in a specific document library (drive)
    # Get site ID, get drive ID
    # DRIVE_NAME = "Documents"
    #======================================================================================================
    @task
    def list_items_in_drive(**context) -> None:
        """
        List all items in a specific document library (drive) in a SharePoint site using Microsoft Graph API.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}

        # Get site ID
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SharePoint site: {site_url}")
        response = requests.get(site_url, headers=headers)
        response.raise_for_status()
        site_id = response.json()["id"]
        logger.info(f"Site ID: {site_id}")

        # Get drive ID for the specified document library
        drives_url = f"{GRAPH_API_BASE}/sites/{site_id}/drives"
        response = requests.get(drives_url, headers=headers)
        response.raise_for_status()
        drives = response.json().get('value', [])
        drive_id = next((d['id'] for d in drives if d['name'] == DRIVE_NAME), None)
        if not drive_id:
            logger.error(f"Drive '{DRIVE_NAME}' not found.")
            return
        logger.info(f"Drive ID for '{DRIVE_NAME}': {drive_id}")

        # List all items in the specified drive
        items_url = f"{GRAPH_API_BASE}/drives/{drive_id}/root/children"
        response = requests.get(items_url, headers=headers)
        logger.info(f"Requesting items from drive {DRIVE_NAME}: {items_url}")
        response.raise_for_status()
        items = response.json().get('value', [])
        logger.info(f"Items found in '{DRIVE_NAME}': {len(items)}")
        for item in items:
            logger.info(f"Item: {item.get('name')}")

    #=======================================================================================================
    # Task. Connect to team SharePoint site and list items in a specific document library (drive) folder
    # Get site ID, get drive ID
    # DRIVE_NAME = "Documents", FOLDER_PATH = "anya_test_delete"
    #======================================================================================================
    @task
    def list_documents_in_drive_folder(**context) -> None:
        """
        Connect to a SharePoint site, access a drive folder, and list all documents in that folder.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}

        # Get site ID
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SharePoint site: {site_url}")
        response = requests.get(site_url, headers=headers)
        response.raise_for_status()
        site_id = response.json()["id"]
        logger.info(f"Site ID: {site_id}")

        # Get drive ID for the specified document library
        drives_url = f"{GRAPH_API_BASE}/sites/{site_id}/drives"
        response = requests.get(drives_url, headers=headers)
        response.raise_for_status()
        drives = response.json().get('value', [])
        drive_id = next((d['id'] for d in drives if d['name'] == DRIVE_NAME), None)
        if not drive_id:
            logger.error(f"Drive '{DRIVE_NAME}' not found.")
            return
        logger.info(f"Drive ID for '{DRIVE_NAME}': {drive_id}")

        # List documents in the drive folder
        url = f"{GRAPH_API_BASE}/sites/{site_id}/drives/{drive_id}/root:/{FOLDER_PATH}:/children"
        response = requests.get(url, headers=headers)
        logger.info(f"Requesting items from drive {DRIVE_NAME} folder {FOLDER_PATH}: {url}")
        response.raise_for_status()
        items = response.json().get('value', [])
        logger.info(f"Items found in '{FOLDER_PATH}': {len(items)}")
        for item in items:
            logger.info(f"Item: {item.get('name')}")


    #sequence of tasks:

    ##OneDrive
    # check_input_in_onedrive()

    ##SharePoint site
    # connect_to_sharepoint_site()
    # list_document_libraries()
    # list_items_in_drive()
    list_documents_in_drive_folder()


# Instantiate the DAG
dag = connect_to_MS365_resources()
#=======================================================================================================