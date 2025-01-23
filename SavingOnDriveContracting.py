import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta


class SavingOnDriveContracting:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None

    def authenticate(self):
        try:
            print("Authenticating Google Drive...")
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            self.service = build("drive", "v3", credentials=creds)
            print("Authentication successful.")
        except Exception as e:
            print(f"Authentication failed: {e}")
            raise

    def create_folder(self, folder_name, parent_folder_id=None):
        print(f"Attempting to create folder: {folder_name}")
        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
        }
        if parent_folder_id:
            file_metadata["parents"] = [parent_folder_id]

        folder = self.service.files().create(body=file_metadata, fields="id").execute()
        print(f"Folder '{folder_name}' created with ID: {folder.get('id')}")
        return folder.get("id")


    def get_folder_id(self, folder_name):
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        response = self.service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        folders = response.get('files', [])
        if folders:
            return folders[0]['id']
        return None

    def upload_file(self, file_name, folder_id):
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media = MediaFileUpload(file_name, resumable=True)
        file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')

    def save_files(self, files, folder_id=None):
        try:
            parent_folder_id = "1HDaiX9adrEsAx74dRlbmgMZMm_eeVyHM"
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

            if folder_id is None:
                folder_id = self.get_folder_id(yesterday)
                if not folder_id:
                    print(f"Parent folder ID: {parent_folder_id}")
                    try:
                        self.service.files().get(fileId=parent_folder_id).execute()
                    except Exception as e:
                        print(f"Parent folder validation failed: {e}")
                        raise
                    folder_id = self.create_folder(yesterday, parent_folder_id)

            for file_name in files:
                if os.path.exists(file_name):
                    print(f"Uploading file: {file_name}")
                    file_metadata = {"name": os.path.basename(file_name), "parents": [folder_id]}
                    media = MediaFileUpload(file_name, resumable=True)
                    self.service.files().create(body=file_metadata, media_body=media, fields="id").execute()
                    print(f"File uploaded: {file_name}")
                else:
                    print(f"File not found: {file_name}")
        except Exception as e:
            print(f"Error in save_files: {e}")
            raise

    def list_files(self):
        try:
            print("Listing files on Google Drive...")
            results = self.service.files().list(pageSize=10, fields="files(id, name)").execute()
            for file in results.get("files", []):
                print(f"File: {file['name']} (ID: {file['id']})")
        except Exception as e:
            print(f"Error listing files: {e}")
