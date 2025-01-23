import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta
import traceback


class SavingOnDriveContracting:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict
        self.scopes = ['https://www.googleapis.com/auth/drive']
        self.service = None

    # def authenticate(self):
    #     # Load credentials directly from the JSON content
    #     creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
    #     self.service = build('drive', 'v3', credentials=creds)

    def authenticate(self):
        try:
            # Print out credentials details for debugging
            print("Credentials Dict Keys:", list(self.credentials_dict.keys()))
            print("Service Account Email:", self.credentials_dict.get('client_email'))
            
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            self.service = build('drive', 'v3', credentials=creds)
            
            # Verify service is working by listing files
            test_files = self.service.files().list(pageSize=10, fields="nextPageToken, files(id, name)").execute()
            print("Successfully authenticated and can list files")
        except Exception as e:
            print("Authentication Error:")
            print(traceback.format_exc())
            raise
    
    def create_folder(self, folder_name, parent_folder_id=None):
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        folder = self.service.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')

    def upload_file(self, file_name, folder_id):
        file_metadata = {'name': file_name, 'parents': [folder_id]}
        media = MediaFileUpload(file_name, resumable=True)
        file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')

    # def save_files(self, files, folder_id=None):
    #     """
    #     Upload files to Google Drive.
        
    #     Args:
    #         files (list): List of file names to upload
    #         folder_id (str, optional): ID of the folder to upload to. If None, creates a new folder
    #     """
    #     if folder_id is None:
    #         parent_folder_id = '1HDaiX9adrEsAx74dRlbmgMZMm_eeVyHM'  # ID of "Property Scraper Uploads"
    #         yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    #         folder_id = self.create_folder(yesterday, parent_folder_id)

    #     for file_name in files:
    #         self.upload_file(file_name, folder_id)
        
    #     return folder_id
    
    def save_files(self, files, folder_id=None):
        try:
            if folder_id is None:
                parent_folder_id = '1HDaiX9adrEsAx74dRlbmgMZMm_eeVyHM'
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                folder_id = self.create_folder(yesterday, parent_folder_id)
                print(f"Created folder {yesterday} with ID: {folder_id}")
            
            for file_name in files:
                try:
                    if not os.path.exists(file_name):
                        print(f"Error: File {file_name} does not exist")
                        continue
                    
                    file_metadata = {'name': os.path.basename(file_name), 'parents': [folder_id]}
                    media = MediaFileUpload(file_name, resumable=True)
                    file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                    print(f"Uploaded {file_name}, file ID: {file.get('id')}")
                except Exception as upload_error:
                    print(f"Detailed upload error for {file_name}:")
                    print(traceback.format_exc())
                    raise
            
            return folder_id
        except Exception as e:
            print("Comprehensive save_files error:")
            print(traceback.format_exc())
            raise
    
    def get_folder_id(self, folder_name):
        try:
            query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            response = self.service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
            folders = response.get('files', [])
            if folders:
                return folders[0]['id']
            return None
        except Exception as e:
            print(f"Error fetching folder ID for '{folder_name}': {e}")
            return None
