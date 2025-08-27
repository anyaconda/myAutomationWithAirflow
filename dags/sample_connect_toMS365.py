#=======================================================================================================
#meta 8/18/2025 [sanitized] Sample Connect DAG to MS365 resources: OneDrive and SharePoint 
# This DAG connects to Microsoft 365 resources (OneDrive and SharePoint) to check for input files.
# Provides a sample on how to connect to the same MS365 product and different file mgmt resources.
# Started from Airflow project 'anya-name', dags/anya-name-dag.py (see details at the end)


#history 8/18/2025 SAMPLE CONNECT DAG TO OneDrive and SPSite
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

## $sanit DOMAIN, REMOVED, GROUP

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
# from utils.utils_MS365 import (
#     get_access_token,
#     upload_file,
#     delete_file,
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
SOURCE_FOLDER: str = CONFIG["source_folder"] #"https://DOMAIN.sharepoint.com/:f:/r/sites/GROUP/Shared%20Documents/anya_test_delete?REMOVED" 
USER_ID: str = CONFIG[""]
#=======================================================================================================
# SPSite Variables
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
    # Task. Connect to our team SPSite
    # Get site ID
    # SITE_NAME = "GROUP"
    #======================================================================================================
    @task
    def connect_to_SPSite(**context) -> None:
        """
        Connect to SPSite and get SPSite ID.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}

        # SPSite VARS hardcoded above
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
        response = requests.get(site_url, headers=headers)
        #Get the site ID for your SPSite:
        site_id = response.json()["id"]
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Got SPSite ID")

    #=======================================================================================================
    # Task. Connect to team SPSite and list Document Libraries
    # Get site ID
    # use endpoints like: 
        # /sites/{site-id}/lists — List all SharePoint lists 
        # /sites/{site-id}/drives — List all document libraries 
        # /sites/{site-id}/users — List all users with access 
        # /sites/{site-id}/columns — List all site columns 
        # /sites/{site-id}/contentTypes — List all content types 
    #======================================================================================================
    @task
    def list_SPSite_document_libraries(**context) -> None:
        """
        List all document libraries in a SPSite using Microsoft Graph API.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}

        # Get site ID
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
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
    # Task. Connect to team SPSite and list items in a specific document library (drive)
    # Get site ID, get drive ID
    # DRIVE_NAME = "Documents"
    #======================================================================================================
    @task
    def list_items_in_SPSite_drive(**context) -> None:
        """
        List all items in a specific document library (drive) in a SPSite using Microsoft Graph API.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}

        # Get site ID
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
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
    # Task. Connect to team SPSite and list items in a specific document library (drive) folder
    # Get site ID, get drive ID
    # DRIVE_NAME = "Documents", FOLDER_PATH = "anya_test_delete"
    #======================================================================================================
    @task
    def list_documents_in_SPSite_drive_folder(**context) -> None:
        """
        Connect to a SPSite, access a drive folder, and list all documents in that folder.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}

        # Get site ID
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
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

    #=======================================================================================================
    # Task. Connect to team SPSite and read a document in a specific document library (drive) folder
    # Get site ID, get drive ID
    # DRIVE_NAME = "Documents", FOLDER_PATH = "anya_test_delete"
    #======================================================================================================
    @task
    def read_document_in_SPSite_drive_folder(**context) -> None: 
        """
        Connect to a SPSite, access a drive folder, and read a document in that folder.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}

        # Get site ID
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
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

        #read 1st document in the folder
        file_url = f"{GRAPH_API_BASE}/sites/{site_id}/drives/{drive_id}/root:/{FOLDER_PATH}:/children/{items[0].get('name')}/content"
        logger.info(f"Using file path for download: {file_url}")
        response = requests.get(file_url, headers=headers)
        response.raise_for_status()
        #check if they csv file was successfully read by making it into a df
        df = pd.read_csv(io.BytesIO(response.content))
        logger.info(df.shape)
        logger.info(df.columns)


    #=======================================================================================================
    # Task. Write a document to a SPSite document library (drive) folder
    # Get site ID, get drive ID
    # DRIVE_NAME = "Documents", FOLDER_PATH = "anya_test_delete"
    #======================================================================================================
    @task
    def write_document_to_SPSite_drive_folder(**context) -> None: 
        """
        Write a document to a SPSite drive folder.
        """
        access_token = get_access_token(CLIENT_ID, TENANT_ID, USERNAME, PASSWORD)
        headers = {"Authorization": f"Bearer {access_token}"}

        # Get site ID
        site_url = f"{GRAPH_API_BASE}/sites/{DOMAIN}.sharepoint.com:/sites/{SITE_NAME}"
        logger.info(f"Connecting to {SITE_NAME} SPSite: {site_url}")
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
        #$manual hardcoded URL - $note straight to drive
        #upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}}/root:/anya_test_delete/{file_out}:/content"
        #$dynamic URL
        upload_url = f"{GRAPH_API_BASE}/drives/{drive_id}/root:/{FOLDER_PATH}/{file_out}:/content"
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