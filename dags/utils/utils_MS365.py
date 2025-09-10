#=======================================================================================================
#meta 9/2/2025 Utils for MS365 Access (OneDrive and SharePoint) via Microsoft Graph API
# Started from Airflow project 'anya-name', dags/utils/utils_MS365.py
#  prev started from 'onedrive_helper.py', modify for this project needs


#previously in original utils_MS365.py
#8/25/2025 NAME DAG WRITE OUTPUT TO SPSite
#      $delta_name_dag0
#      Write differences report to Name SPSite destination folder
#      temporarily use hardcoded drive ID and folder until resolve 403 error

#history 
#9/2/2025 UTILS FOR MS365
#      Modular helper methods for OneDrive and SharePoint

#9/5/2025 ADDED CUSTOM UTILS LIBRARY, COPY FILE
#      $delta_sample_connect_dag3
#      Added: Copy a document within a SPSite/drive (a longer version)
#      Enabled utils in 'sample_connect_toMS365' DAG: Write a document to a SPSite document library (drive) folder

#9/8/2025 ADDED CUSTOM UTILS LIBRARY, DELETE FILE
#      $delta_sample_connect_dag3a
#      Tried: Copy a document within a SPSite/drive ('utils.copy_file' a shorter version)
#      For now sticking with a longer version
#      Added: Delete a document within a SPSite/drive
#
#      Ready for Airflow qa test and merge


#tags $debug $manual $error 403
## $sanit NAME, DOMAIN, REMOVED, GROUP


#References:
#How to write a working URL from 'How to upload a large document in c# using the Microsoft Graph API rest calls'
# refer to https://stackoverflow.com/questions/49776955/how-to-upload-a-large-document-in-c-sharp-using-the-microsoft-graph-api-rest-call
# similar in 'Have to upload files into Sharepoint online using Graph API'
# refer to https://learn.microsoft.com/en-us/answers/questions/938993/have-to-upload-files-into-sharepoint-online-using
#=======================================================================================================

#=======================================================================================================
# Standard library imports
import os
import requests
import pandas as pd
import io

#=======================================================================================================
# Project definitions
__all__ = [
    'get_access_token',
    'upload_file',
    'copy_file',
    'delete_file',
    'download_csv_by_path'
]
#=======================================================================================================
# DEFINE UTILS
#=======================================================================================================
def get_access_token(client_id, tenant_id, username, password):
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
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

def download_csv_by_path(file_path, access_token):
    """
    Download a CSV file from OneDrive or SPSite by its path and 
    return pandas dataframe.
    """
    headers = {"Authorization": f"Bearer {access_token}"}
    #url_OneDrive = f"{graph_api_base}/users/{user_id}/drive/root:/{file_path}:/content"
    #response = requests.get(url_OneDrive, headers=headers)     # delta_name_dag0
    response = requests.get(file_path, headers=headers)
    response.raise_for_status()
    _df = pd.read_csv(io.BytesIO(response.content))
    return _df

def upload_file(folder_path_in, file_or_path_in, access_token, file_content_in=None):
    """
    Upload a file to OneDriveor SPSite. 
    Accepts either a file path (str), a file-like object/bytes, or direct file content (string/bytes) with a filename.
    """

    if file_content_in is not None:
        # file_or_path is expected to be the filename
        _filename = file_or_path_in
        if isinstance(file_content_in, str):
            _data = file_content_in.encode('utf-8')
        else:
            _data = file_content_in
    elif isinstance(file_or_path_in, str):
        _filename = os.path.basename(file_or_path_in)
        _data = open(file_or_path_in, 'rb')
    elif isinstance(file_or_path_in, io.BytesIO):
        file_or_path_in.seek(0)
        raise ValueError("Expected an Excel file.")
        _filename = getattr(file_or_path_in, 'name', 'processed.xlsx')
        _data = file_or_path_in
    elif isinstance(file_or_path_in, bytes):
        raise ValueError("Expected an Excel file.")
        _filename = 'processed.xlsx'
        _data = io.BytesIO(file_or_path)
    else:
        raise ValueError("file_or_path must be a file path, BytesIO, bytes, or provide file_content.")
    
    headers = {"Authorization": f"Bearer {access_token}"}
    #upload_url_OneDrive = f"{graph_api_base}/users/{user_id}/drive/root:/{folder_path}/{filename}:/content"
    #$manual upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/anya_test_delete/{filename}:/content"
    _upload_url = f"{folder_path_in}/{_filename}:/content"

    response = requests.put(_upload_url, headers=headers, data=_data)
    if isinstance(file_or_path_in, str) and file_content_in is None:
        _data.close()
    response.raise_for_status()
    print(f"Uploaded {_filename}")

#delta_sample_connect_dag3 (longer version)
def copy_file(file_path_in, item_id_in, file_path_dest_id_in, access_token):
    #copy file to another location
    #$manual _copy_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/drive/items/{item_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    _copy_url = f"{file_path_in}/items/{item_id_in}/copy"

    _copy_payload = {
        "parentReference": {
            "id": file_path_dest_id_in
        }#,
        #"name": NEW_FILE_NAME
    }

    response = requests.post(_copy_url, headers=headers, json=_copy_payload)
    print(f"Copied item {item_id_in} to folder {file_path_dest_id_in}")
    response.raise_for_status()

#delta_sample_connect_dag3a (shorter version)
#def copy_file(drive_url_in, drive_name_in, folder_path_in, file_name_in, folder_path_new_in, access_token):
    # # List documents in the drive folder (to get IDs)
    # _url = f"{drive_url_in}/root:/{folder_path_in}:/children" #delta_sample_connect_dag1
    # response = requests.get(_url, headers=headers)
    # print(f"Requesting items from drive {drive_name_in} folder {folder_path_in}: {_url}")
    # response.raise_for_status()
    # _folder_items = response.json().get('value', [])
    # print(f"Items found in '{folder_path_in}': {len(_folder_items)}")

    # for _folder_item in _folder_items:
    #     # get file ID to copy
    #     print(f"Item: {_folder_item.get('name')}, {_folder_item.get('id')}")
    #     if _folder_item.get('name') == file_name_in:
    #         _item_id = _folder_item.get('id')
    #         print(f"Found file '{file_name_in}' with ID: {_item_id}")
    #     # Get destination folder ID
    #     if _folder_item.get('name') == folder_path_new_in:
    #         _folder_id = _folder_item.get('id')
    #         print(f"Found folder '{folder_path_new_in}' with ID: {_folder_id}")
    # _copy_url = f"{drive_url_in}/items/{_item_id}/copy"

    # _copy_payload = {
    #     "parentReference": {
    #         "id": _folder_id
    #     }#,
    #     #"name": NEW_FILE_NAME
    # }

    # response = requests.post(_copy_url, headers=headers, json=_copy_payload)
    # print(f"Copied item {file_name_in} to folder {folder_path_new_in}")
    # response.raise_for_status()

#delta_sample_connect_dag3a
def delete_file(folder_path_in, item_id_in, access_token):
    _delete_url = f"{folder_path_in}/items/{item_id_in}"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.delete(_delete_url, headers=headers)
    if response.status_code == 204:
        print(f"Deleted file with ID {item_id_in}.")
    else:
        print(f"Failed to delete file with ID {item_id_in}: {response.status_code} {response.text}")
        response.raise_for_status()


#################################### XTRA ####################################
# # Debug version with print statements to trace the type of input and flow
# def upload_file_debug(user_id, folder_path, file_or_path, access_token, file_content=None):
#     """
#     Upload a file to OneDrive. Accepts either a file path (str), a file-like object/bytes, or direct file content (string/bytes) with a filename.
#     """
#     import io
#     graph_api_base = "https://graph.microsoft.com/v1.0"
#     if file_content is not None:
#         # file_or_path is expected to be the filename
#         filename = file_or_path
#         if isinstance(file_content, str):
#             print("1st a")
#             data = file_content.encode('utf-8')
#         else:
#             print("1st b")
#             data = file_content
#     elif isinstance(file_or_path, str):
#         print("2nd string")
#         filename = os.path.basename(file_or_path)
#         data = open(file_or_path, 'rb')
#     elif isinstance(file_or_path, io.BytesIO):
#         print("3d BytesIO")
#         file_or_path.seek(0)
#         filename = getattr(file_or_path, 'name', 'processed.xlsx')
#         data = file_or_path
#     elif isinstance(file_or_path, bytes):
#         print("4th definitely bytes")
#         filename = 'processed.xlsx'
#         data = io.BytesIO(file_or_path)
#     else:
#         raise ValueError("file_or_path must be a file path, BytesIO, bytes, or provide file_content.")
#     ###upload_url = f"{graph_api_base}/users/{user_id}/drive/root:/{folder_path}/{filename}:/content"
#     print("got here: ", data.__class__)
#     ##$manual hardcoded URL 
#     upload_url = "https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/anya_test_delete/sample.csv:/content" #/items/root:/" + fileName + ":/content";
#     ##$debug upload_url = "https://graph.microsoft.com/v1.0/drives/b!tECiAyeOAUSuBuhb_mb7bZInWZsgCBtEn1Iz-VSKOy4FLg6JPSUYR66VnO5-wemV/root:/2_output/sample.txt:/content" # $error requests.exceptions.HTTPError: 403 Client Error: Forbidden for url: 

#     headers = {"Authorization": f"Bearer {access_token}"}
#     response = requests.put(upload_url, headers=headers, data=data)
#     if isinstance(file_or_path, str) and file_content is None:
#         data.close()
#     response.raise_for_status()
#     print(f"Uploaded {filename}")


#################################### XTRA2 ####################################
# DELETED FROM ORIGINAL
# def list_files(user_id, folder_path, access_token):
#     graph_api_base = "https://graph.microsoft.com/v1.0"
#     url = f"{graph_api_base}/users/{user_id}/drive/root:/{folder_path}:/children"
#     headers = {"Authorization": f"Bearer {access_token}"}
#     response = requests.get(url, headers=headers)
#     response.raise_for_status()
#     return response.json().get('value', [])

# def download_file(user_id, item_id, access_token):
#     """
#     Download an Excel file from OneDrive and return it as a pandas DataFrame (in memory).
#     """
#     graph_api_base = "https://graph.microsoft.com/v1.0"
#     url = f"{graph_api_base}/users/{user_id}/drive/items/{item_id}/content"
#     headers = {"Authorization": f"Bearer {access_token}"}
#     response = requests.get(url, headers=headers)
#     response.raise_for_status()
#     df = pd.read_excel(io.BytesIO(response.content))
#     return df

# def download_file_by_path(user_id, file_path, access_token):
#     """
#     Download an Excel file from OneDrive by its path and return it as a pandas DataFrame.
#     """
#     graph_api_base = "https://graph.microsoft.com/v1.0"
#     url = f"{graph_api_base}/users/{user_id}/drive/root:/{file_path}:/content"
#     headers = {"Authorization": f"Bearer {access_token}"}
#     response = requests.get(url, headers=headers)
#     response.raise_for_status()
#     df = pd.read_excel(io.BytesIO(response.content))
#     return df

# def download_xlsx_by_path(user_id, file_path, access_token):
#     """
#     Download an Excel file from OneDrive by its path and return the raw bytes (xlsx file).
#     """
#     graph_api_base = "https://graph.microsoft.com/v1.0"
#     url = f"{graph_api_base}/users/{user_id}/drive/root:/{file_path}:/content"
#     headers = {"Authorization": f"Bearer {access_token}"}
#     response = requests.get(url, headers=headers)
#     response.raise_for_status()
#     return response.content

# def download_csv_by_path(user_id, file_path, access_token):
#     """
#     Download a CSV file from OneDrive by its path and return the raw bytes (csv file).
#     """
#     graph_api_base = "https://graph.microsoft.com/v1.0"
#     url = f"{graph_api_base}/users/{user_id}/drive/root:/{file_path}:/content"
#     headers = {"Authorization": f"Bearer {access_token}"}
#     response = requests.get(url, headers=headers)
#     response.raise_for_status()
#     df = pd.read_csv(io.BytesIO(response.content))
#     return df

# def delete_file(user_id, item_id, access_token):
#     graph_api_base = "https://graph.microsoft.com/v1.0"
#     url = f"{graph_api_base}/users/{user_id}/drive/items/{item_id}"
#     headers = {"Authorization": f"Bearer {access_token}"}
#     response = requests.delete(url, headers=headers)
#     if response.status_code == 204:
#         print(f"Deleted file with ID {item_id} from OneDrive input folder.")
#     else:
#         print(f"Failed to delete file with ID {item_id}: {response.status_code} {response.text}")
#         response.raise_for_status()
