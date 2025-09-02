#=======================================================================================================
#meta 8/18/2025 [sanitized] Sample Connect DAG to MS365 resources: OneDrive and SharePoint 
# This DAG connects to Microsoft 365 resources (OneDrive and SharePoint) to check for input files.
# Provides a sample on how to connect to the same MS365 product and different file mgmt resources.
# Started from Airflow project 'anya-name', dags/anya-name-dag.py (see details at the end)


#history 
#8/18/2025 SAMPLE CONNECT DAG TO OneDrive and SPSite
#      (already)Successful setup of local Airflow environment
#      w/ apache/airflow:2.9.1 (downgraded after struggling with apache/airflow:3.0.3)
#      (already) able to connect to OneDrive personal folder
#      Connect to SPSite

#8/19/2025 MODIFIED SAMPLE CONNECT SPSite
#      Modified: connect to SPSite, retrieve site ID, not connecting to any other resources yet
#8/19/2025 ADDED CONNECT TO SPSite -> DRIVE -> FOLDER
#      Added: connecting to SPSite drive and folder

#8/20/2025 ADDED READ DOCUMENT IN SPSite -> DRIVE -> FOLDER
#      Added: connect to SPSite drive and folder, and read a document in that folder.

#8/26/2025 ADDED WRITE DOCUMENT TO SPSite -> DRIVE -> FOLDER
#      $delta_sample_connect_dag0
#      Added: Write a document to a SPSite document library (drive) folder

#9/2/2025 ADDED CUSTOM UTILS LIBRARY, USING AIRFLOW XCOM
#      $delta_sample_connect_dag1, using XCom
#      Added Custom utils library for access and "session" management
#      $temp kept prev vs now
#      added a custom exception NoDriveFound


## $sanit NAME, DOMAIN, REMOVED, GROUP, {user_id}

#References:
#How to write a working URL from 'How to upload a large document in c# using the Microsoft Graph API rest calls'
# refer to https://stackoverflow.com/questions/49776955/how-to-upload-a-large-document-in-c-sharp-using-the-microsoft-graph-api-rest-call
# similar in 'Have to upload files into Sharepoint online using Graph API'
# refer to https://learn.microsoft.com/en-us/answers/questions/938993/have-to-upload-files-into-sharepoint-online-using
#=======================================================================================================

#=======================================================================================================
# Standard library imports
import requests
import pandas as pd
import io
from datetime import datetime

# Third-party imports
from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import Variable #, XCom


# # Project imports
from utils.utils_MS365 import (
    get_access_token,
#     upload_file,
#     delete_file,
#     download_csv_by_path
)

#=======================================================================================================
# Airflow Variables
CLIENT_ID: str = Variable.get("")
TENANT_ID: str = Variable.get("")
USERNAME: str = Variable.get("")
PASSWORD: str = Variable.get("")
#=======================================================================================================
GRAPH_API_BASE = "https://graph.microsoft.com/v1.0"
DOMAIN = "REMOVED"
#=======================================================================================================
# OneDrive Variables
CONFIG = Variable.get("anya_connect_config", deserialize_json=True)
SOURCE_FOLDER: str = CONFIG["source_folder"] #"https://DOMAIN.sharepoint.com/:f:/r/sites/GROUP/Shared%20Documents/anya_test_delete?REMOVED" 
USER_ID: str = CONFIG[""]
#=======================================================================================================
# SPSite Variables
SITE_NAME = "GROUP"
DRIVE_NAME = "Documents"
FOLDER_PATH = "anya_test_delete"
#=======================================================================================================
logger = LoggingMixin().log
 
# Define the default arguments
default_args = {
    'owner': 'airflow',
    # 'email_on_failure': True,
    # 'email': [{user_id}],
}
#=======================================================================================================
    
###################################################################################
###  DAG CODE
#per Copilot 
##In Airflow, you cannot reliably set a Python global variable inside a task and use it in other tasks, 
# because each task runs in its own process (or even on a different worker). Global variables do not persist across tasks.
# 1. Use Airflow XComs:
# Pass data between tasks using XComs (cross-communication objects).
#
    # @task
    # def set_SPSite_ID():
    #     return "variable"

    # @task
    # def get_value(xcom_value):
    #     print(xcom_value)

    # value = set_SPSite_ID()
    # get_value(value) 
#
# 2. Use Airflow Variables:
# For persistent, workspace-wide values, use Airflow Variables:
#
    # from airflow.models import Variable
    # Variable.set("my_global_var", "my_value")
    # value = Variable.get("my_global_var")
##
###################################################################################

#delta_name_dag1 - define a custom exception for drive not found case
class NoDriveFound(Exception):
    """Raised when no drive found in OneDrive or SPSite."""
    def __init__(self, message, value):
        super().__init__(message)
        self.value = value
        logger.info(f"{self}{self.value}")


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
    # Tasks 0. Connect to team SPSite, get and set SPSite global vars for reuse.
    # SITE_NAME = "<SITE_NAME>", DRIVE_NAME = "Rates Comparator Docs"
    # Get and set site ID, drive ID
    # delta_name_dag0
    #======================================================================================================

    @task #delta_name_dag1 using XCom
    def set_SPSite_info():
        """
        Connect to a SPSite, get and set SPSite global vars for reuse.
        Return site and drive info
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}

        # Get site ID
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
        response = requests.get(site_url, headers=headers)
        response.raise_for_status()
        _site_id = response.json()["id"]
        _site_url = f"{GRAPH_API_BASE}/sites/{_site_id}"
        logger.info(f"Site ID: {_site_id}")

        # Get drive ID for the specified document library
        drives_url = f"{GRAPH_API_BASE}/sites/{_site_id}/drives"
        response = requests.get(drives_url, headers=headers)
        response.raise_for_status()
        drives = response.json().get('value', [])
        _drive_id = next((d['id'] for d in drives if d['name'] == DRIVE_NAME), None)
        # if no drive found, error out
        if not _drive_id:
            raise NoDriveFound("Drive not found: ", DRIVE_NAME)
        logger.info(f"Drive ID for '{DRIVE_NAME}': {_drive_id}")   

        # Get drive URL #delta_name_dag1a
        _drive_url = f"{GRAPH_API_BASE}/drives/{_drive_id}"

        return [_site_id, _site_url, _drive_id, _drive_url]

    @task
    def get_SPSite_info(xcom_value: list):
        site_id, site_url, drive_id, drive_url = xcom_value
        logger.info(f"Site ID: {site_id}, Site URL: {site_url}") #delta_name_dag1a
        logger.info(f"Drive ID: {drive_id}, Drive URL: {drive_url}")

    
    #=======================================================================================================
    # Task. Connect to our team SPSite
    # SITE_NAME = "GROUP"
    #======================================================================================================
    @task
    # def connect_to_SPSite(**context) -> None:
    #     """
    #     Connect to SPSite and get SPSite ID.
    #     """
    #     access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
    #     headers = {"Authorization": f"Bearer {access_token}"}

    #     # SPSite VARS hardcoded above
    #     site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
    #     logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
    #     response = requests.get(site_url, headers=headers)
    #     #Get the site ID for your SPSite:
    #     site_id = response.json()["id"]
    #     logger.info(f"Response status code: {response.status_code}")
    #     logger.info(f"Got SPSite ID")

     #delta_name_dag1 using XCom
    def connect_to_SPSite(SPSITE_INFO) -> None:
        _site_id, _site_url, _drive_id, _drive_url = SPSITE_INFO
        logger.info(f"SPSite info for {SITE_NAME}: {_site_id}, {_site_url}")


    #=======================================================================================================
    # Task. Connect to team SPSite and list Document Libraries
    # use endpoints like: 
        # /sites/{site-id}/lists — List all SharePoint lists 
        # /sites/{site-id}/drives — List all document libraries 
        # /sites/{site-id}/users — List all users with access 
        # /sites/{site-id}/columns — List all site columns 
        # /sites/{site-id}/contentTypes — List all content types 
    #======================================================================================================
    @task
    # def list_SPSite_document_libraries(**context) -> None:
    #     """
    #     List all document libraries in a SPSite using Microsoft Graph API.
    #     """
    #     access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
    #     headers = {"Authorization": f"Bearer {access_token}"}

    #     # Get site ID
    #     site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
    #     logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
    #     response = requests.get(site_url, headers=headers)
    #     response.raise_for_status()
    #     site_id = response.json()["id"]
    #     logger.info(f"Site ID: {site_id}")

    #     # List all document libraries (drives)
    #     url = f"{GRAPH_API_BASE}/sites/{site_id}/drives"
    #     logger.info(f"Requesting document libraries from site {SITE_NAME}: {url}")
    #     response = requests.get(url, headers=headers)
    #     #logger.info(f"Response status code: {response.status_code}")
    #     response.raise_for_status()
    #     drives = response.json().get('value', [])
    #     logger.info(f"Document libraries found: {len(drives)}")
    #     for drive in drives:
    #         logger.info(f"Library: {drive.get('name')} | ID: {drive.get('id')}")

    #delta_name_dag1 using XCom
    def list_SPSite_document_libraries(SPSITE_INFO) -> None:
        """
        List all document libraries in a SPSite using Microsoft Graph API.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}
        _site_id, _site_url, _drive_id, _drive_url = SPSITE_INFO

        # List all document libraries (drives)
        url = f"{_site_url}/drives"
        logger.info(f"Requesting document libraries from site {SITE_NAME}: {url}")
        response = requests.get(url, headers=headers)
        #logger.info(f"Response status code: {response.status_code}")
        response.raise_for_status()
        drives = response.json().get('value', [])
        logger.info(f"Document libraries found: {len(drives)}")
        for drive in drives:
            logger.info(f"Library: {drive.get('name')} | ID: {drive.get('id')}")
    
    #=======================================================================================================
    # Task. Connect to team SPSite and list items in a specific document library (drive)
    # DRIVE_NAME = "Documents"
    #======================================================================================================
    @task
    # def list_items_in_SPSite_drive(**context) -> None:
    #     """
    #     List all items in a specific document library (drive) in a SPSite using Microsoft Graph API.
    #     """
    #     access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
    #     headers = {"Authorization": f"Bearer {access_token}"}

    #     # Get site ID
    #     site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
    #     logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
    #     response = requests.get(site_url, headers=headers)
    #     response.raise_for_status()
    #     site_id = response.json()["id"]
    #     logger.info(f"Site ID: {site_id}")

    #     # Get drive ID for the specified document library
    #     drives_url = f"{GRAPH_API_BASE}/sites/{site_id}/drives"
    #     response = requests.get(drives_url, headers=headers)
    #     response.raise_for_status()
    #     drives = response.json().get('value', [])
    #     drive_id = next((d['id'] for d in drives if d['name'] == DRIVE_NAME), None)
    #     if not drive_id:
    #         logger.error(f"Drive '{DRIVE_NAME}' not found.")
    #         return
    #     logger.info(f"Drive ID for '{DRIVE_NAME}': {drive_id}")

    #     # List all items in the specified drive
    #     items_url = f"{GRAPH_API_BASE}/drives/{drive_id}/root/children"
    #     response = requests.get(items_url, headers=headers)
    #     logger.info(f"Requesting items from drive {DRIVE_NAME}: {items_url}")
    #     response.raise_for_status()
    #     items = response.json().get('value', [])
    #     logger.info(f"Items found in '{DRIVE_NAME}': {len(items)}")
    #     for item in items:
    #         logger.info(f"Item: {item.get('name')}")

    #delta_name_dag1 using XCom
    def list_items_in_SPSite_drive(SPSITE_INFO) -> None:
        """
        List all items in a specific document library (drive) in a SPSite using Microsoft Graph API.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}
        _site_id, _site_url, _drive_id, _drive_url = SPSITE_INFO

        # List all items in the specified drive
        items_url = f"{_drive_url}/root/children" #delta_sample_connect_dag1
        response = requests.get(items_url, headers=headers)
        logger.info(f"Requesting items from drive {DRIVE_NAME}: {items_url}")
        response.raise_for_status()
        items = response.json().get('value', [])
        logger.info(f"Items found in '{DRIVE_NAME}': {len(items)}")
        for item in items:
            logger.info(f"Item: {item.get('name')}")

    #=======================================================================================================
    # Task. Connect to team SPSite and list items in a specific document library (drive) folder
    # DRIVE_NAME = "Documents", FOLDER_PATH = "anya_test_delete"
    #======================================================================================================
    @task
    # def list_documents_in_SPSite_drive_folder(**context) -> None:
    #     """
    #     Connect to a SPSite, access a drive folder, and list all documents in that folder.
    #     """
    #     access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
    #     headers = {"Authorization": f"Bearer {access_token}"}

    #     # Get site ID
    #     site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
    #     logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
    #     response = requests.get(site_url, headers=headers)
    #     response.raise_for_status()
    #     site_id = response.json()["id"]
    #     logger.info(f"Site ID: {site_id}")

    #     # Get drive ID for the specified document library
    #     drives_url = f"{GRAPH_API_BASE}/sites/{site_id}/drives"
    #     response = requests.get(drives_url, headers=headers)
    #     response.raise_for_status()
    #     drives = response.json().get('value', [])
    #     drive_id = next((d['id'] for d in drives if d['name'] == DRIVE_NAME), None)
    #     if not drive_id:
    #         logger.error(f"Drive '{DRIVE_NAME}' not found.")
    #         return
    #     logger.info(f"Drive ID for '{DRIVE_NAME}': {drive_id}")

    #     # List documents in the drive folder
    #     url = f"{GRAPH_API_BASE}/sites/{site_id}/drives/{drive_id}/root:/{FOLDER_PATH}:/children"
    #     response = requests.get(url, headers=headers)
    #     logger.info(f"Requesting items from drive {DRIVE_NAME} folder {FOLDER_PATH}: {url}")
    #     response.raise_for_status()
    #     items = response.json().get('value', [])
    #     logger.info(f"Items found in '{FOLDER_PATH}': {len(items)}")
    #     for item in items:
    #         logger.info(f"Item: {item.get('name')}")

    #delta_name_dag1 using XCom
    def list_documents_in_SPSite_drive_folder(SPSITE_INFO) -> None:
        """
        Connect to a SPSite, access a drive folder, and list all documents in that folder.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}
        _site_id, _site_url, _drive_id, _drive_url = SPSITE_INFO

        # List documents in the drive folder
        url = f"{_drive_url}/root:/{FOLDER_PATH}:/children" #delta_sample_connect_dag1
        response = requests.get(url, headers=headers)
        logger.info(f"Requesting items from drive {DRIVE_NAME} folder {FOLDER_PATH}: {url}")
        response.raise_for_status()
        items = response.json().get('value', [])
        logger.info(f"Items found in '{FOLDER_PATH}': {len(items)}")
        for item in items:
            logger.info(f"Item: {item.get('name')}")

    #=======================================================================================================
    # Task. Connect to team SPSite and read a document in a specific document library (drive) folder
    # DRIVE_NAME = "Documents", FOLDER_PATH = "anya_test_delete"
    #======================================================================================================
    @task
    # def read_document_in_SPSite_drive_folder(**context) -> None: 
    #     """
    #     Connect to a SPSite, access a drive folder, and read a document in that folder.
    #     """
    #     access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
    #     headers = {"Authorization": f"Bearer {access_token}"}

    #     # Get site ID
    #     site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
    #     logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
    #     response = requests.get(site_url, headers=headers)
    #     response.raise_for_status()
    #     site_id = response.json()["id"]
    #     logger.info(f"Site ID: {site_id}")

    #     # Get drive ID for the specified document library
    #     drives_url = f"{GRAPH_API_BASE}/sites/{site_id}/drives"
    #     response = requests.get(drives_url, headers=headers)
    #     response.raise_for_status()
    #     drives = response.json().get('value', [])
    #     drive_id = next((d['id'] for d in drives if d['name'] == DRIVE_NAME), None)
    #     if not drive_id:
    #         logger.error(f"Drive '{DRIVE_NAME}' not found.")
    #         return
    #     logger.info(f"Drive ID for '{DRIVE_NAME}': {drive_id}")

    #     # List documents in the drive folder
    #     url = f"{GRAPH_API_BASE}/sites/{site_id}/drives/{drive_id}/root:/{FOLDER_PATH}:/children"
    #     response = requests.get(url, headers=headers)
    #     logger.info(f"Requesting items from drive {DRIVE_NAME} folder {FOLDER_PATH}: {url}")
    #     response.raise_for_status()
    #     items = response.json().get('value', [])
    #     logger.info(f"Items found in '{FOLDER_PATH}': {len(items)}")
    #     for item in items:
    #         logger.info(f"Item: {item.get('name')}")

    #     #read 1st document in the folder
    #     file_url = f"{GRAPH_API_BASE}/sites/{site_id}/drives/{drive_id}/root:/{FOLDER_PATH}:/children/{items[0].get('name')}/content"
    #     logger.info(f"Using file path for download: {file_url}")
    #     response = requests.get(file_url, headers=headers)
    #     response.raise_for_status()
    #     #check if they csv file was successfully read by making it into a df
    #     df = pd.read_csv(io.BytesIO(response.content))
    #     logger.info(df.shape)
    #     logger.info(df.columns)

    #delta_name_dag1 using XCom
    def read_document_in_SPSite_drive_folder(SPSITE_INFO) -> None: 
        """
        Connect to a SPSite, access a drive folder, and read a document in that folder.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}
        _site_id, _site_url, _drive_id, _drive_url = SPSITE_INFO

        # List documents in the drive folder
        url = f"{_drive_url}/root:/{FOLDER_PATH}:/children" #delta_sample_connect_dag1
        response = requests.get(url, headers=headers)
        logger.info(f"Requesting items from drive {DRIVE_NAME} folder {FOLDER_PATH}: {url}")
        response.raise_for_status()
        items = response.json().get('value', [])
        logger.info(f"Items found in '{FOLDER_PATH}': {len(items)}")
        for item in items:
            logger.info(f"Item: {item.get('name')}")

        #read 1st document in the folder
        file_url = f"{_drive_url}/root:/{FOLDER_PATH}:/children/{items[0].get('name')}/content" #delta_sample_connect_dag1
        logger.info(f"Using file path for download: {file_url}")
        response = requests.get(file_url, headers=headers)
        response.raise_for_status()
        #check if the .csv file was successfully read by making it into a df; seems to work ok with a simple .txt file (which is not .csv)
        df = pd.read_csv(io.BytesIO(response.content))
        logger.info(df.shape)
        logger.info(df.columns)


    #=======================================================================================================
    # Task. Write a document to a SPSite document library (drive) folder
    # DRIVE_NAME = "Documents", FOLDER_PATH = "anya_test_delete"
    #======================================================================================================
    @task
    # def write_document_to_SPSite_drive_folder(**context) -> None: 
    #     """
    #     Write a document to a SPSite drive folder.
    #     """
    #     access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
    #     headers = {"Authorization": f"Bearer {access_token}"}

    #     # Get site ID
    #     site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
    #     logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
    #     response = requests.get(site_url, headers=headers)
    #     response.raise_for_status()
    #     site_id = response.json()["id"]
    #     logger.info(f"Site ID: {site_id}")

    #     # Get drive ID for the specified document library
    #     drives_url = f"{GRAPH_API_BASE}/sites/{site_id}/drives"
    #     response = requests.get(drives_url, headers=headers)
    #     response.raise_for_status()
    #     drives = response.json().get('value', [])
    #     drive_id = next((d['id'] for d in drives if d['name'] == DRIVE_NAME), None)
    #     if not drive_id:
    #         logger.error(f"Drive '{DRIVE_NAME}' not found.")
    #         return
    #     logger.info(f"Drive ID for '{DRIVE_NAME}': {drive_id}")
        
    #     # Prepare a sample string or text file for upload
    #     file_name = 'sample'
    #     ext = '.txt'
    #     timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    #     file_out = f"{timestamp}_{file_name}{ext}"
    #     #content options
    #     string_data = "This is the string data to be sent in the PUT request body."
    #     # txt_buffer = io.StringIO()
    #     # txt_buffer.write("Sample text content")
    #     # txt_buffer.seek(0)
    #     # logger.info(f"Buffer content type: {txt_buffer.getvalue().__class__}") #<class 'str'>
        
    #     logger.info(f"Uploading file as {file_out} to destination folder: {FOLDER_PATH}.")

    #     # Upload file delta_sample_connect_dag0
    #     #$next utils.upload_file(USER_ID, FOLDER_PATH, file_out, access_token, file_content=txt_buffer.getvalue())

    #     #How to write a working URL from 'How to upload a large document in c# using the Microsoft Graph API rest calls'
    #     #refer to https://stackoverflow.com/questions/49776955/how-to-upload-a-large-document-in-c-sharp-using-the-microsoft-graph-api-rest-call
    #     #$manual hardcoded URL - $note straight to drive
    #     #upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}}/root:/anya_test_delete/{file_out}:/content"
    #     #$dynamic URL
    #     upload_url = f"{GRAPH_API_BASE}/drives/{drive_id}/root:/{FOLDER_PATH}/{file_out}:/content"
    #     logger.info(f"Uploading file to: {upload_url}")
    #     response = requests.put(upload_url, headers=headers, data=string_data) #txt_buffer.getvalue().encode('utf-8')) #content option
    #     response.raise_for_status()
    #     print(f"Uploaded {file_out} to SharePoint drive '{DRIVE_NAME}' in folder '{FOLDER_PATH}'.")

    #delta_name_dag1 using XCom
    def write_document_to_SPSite_drive_folder(SPSITE_INFO) -> None: 
        """
        Write a document to a SPSite drive folder.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}
        _site_id, _site_url, _drive_id, _drive_url = SPSITE_INFO
        
        # Prepare a sample string or text file for upload
        file_name = 'sample'
        ext = '.txt'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        file_out = f"{timestamp}_{file_name}{ext}"
        #content options
        string_data = "This is the string data to be sent in the PUT request body."
        # txt_buffer = io.StringIO()
        # txt_buffer.write("Sample text content")
        # txt_buffer.seek(0)
        # logger.info(f"Buffer content type: {txt_buffer.getvalue().__class__}") #<class 'str'>
        
        logger.info(f"Uploading file as {file_out} to destination folder: {FOLDER_PATH}.")

        # Upload file delta_sample_connect_dag0
        #$next utils.upload_file(USER_ID, FOLDER_PATH, file_out, access_token, file_content=txt_buffer.getvalue())

        #How to write a working URL from 'How to upload a large document in c# using the Microsoft Graph API rest calls'
        #refer to https://stackoverflow.com/questions/49776955/how-to-upload-a-large-document-in-c-sharp-using-the-microsoft-graph-api-rest-call
        #$manual hardcoded URL- $note straight to drive
        #upload_url = f"https://graph.microsoft.com/v1.0/drives/b!ZH6YF9MJTE23ZjOVv-fEIpInWZsgCBtEn1Iz-VSKOy7HZXuDhfGETIbfD-wc2Wa9/root:/anya_test_delete/{file_out}:/content"
        #$dynamic URL
        upload_url = f"{_drive_url}/root:/{FOLDER_PATH}/{file_out}:/content" #delta_sample_connect_dag1
        logger.info(f"Uploading file to: {upload_url}")
        response = requests.put(upload_url, headers=headers, data=string_data) #txt_buffer.getvalue().encode('utf-8')) #content option
        response.raise_for_status()
        print(f"Uploaded {file_out} to SharePoint drive '{DRIVE_NAME}' in folder '{FOLDER_PATH}'.")


    #sequence of tasks:

    ##OneDrive
    # check_input_in_OneDrive()

    ##SPSite
    # connect_to_SPSite()
    # list_SPSite_document_libraries()
    # list_SPSite_items_in_drive()
    list_documents_in_SPSite_drive_folder()
    read_document_in_SPSite_drive_folder()
    write_document_to_SPSite_drive_folder()


# Instantiate the DAG
dag = connect_to_MS365_resources()
#=======================================================================================================