# import os
# import json
# from google.oauth2.service_account import Credentials
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaFileUpload
# from datetime import datetime, timedelta


# class SavingOnDriveServices:
#     def __init__(self, credentials_dict):
#         self.credentials_dict = credentials_dict
#         self.scopes = ['https://www.googleapis.com/auth/drive']
#         self.service = None

#     def authenticate(self):
#         # Load credentials directly from the JSON content
#         creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
#         self.service = build('drive', 'v3', credentials=creds)

#     def create_folder(self, folder_name, parent_folder_id=None):
#         file_metadata = {
#             'name': folder_name,
#             'mimeType': 'application/vnd.google-apps.folder'
#         }
#         if parent_folder_id:
#             file_metadata['parents'] = [parent_folder_id]

#         folder = self.service.files().create(body=file_metadata, fields='id').execute()
#         return folder.get('id')

#     def upload_file(self, file_name, folder_id):
#         file_metadata = {'name': file_name, 'parents': [folder_id]}
#         media = MediaFileUpload(file_name, resumable=True)
#         file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
#         return file.get('id')

#     def save_files(self, files):
#         parent_folder_id = '1dwoFxJ4F56HIfaUrRk3QufXDE1QlotzA'  # ID of "Property Scraper Uploads"

#         yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
#         folder_id = self.create_folder(yesterday, parent_folder_id)

#         for file_name in files:
#             self.upload_file(file_name, folder_id)
#         print(f"Files uploaded successfully to folder '{yesterday}' on Google Drive.")

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
        self.parent_folder_id = '1dwoFxJ4F56HIfaUrRk3QufXDE1QlotzA'  # Your parent folder ID

    def authenticate(self):
        """Authenticate with Google Drive API."""
        try:
            print("Authenticating with Google Drive...")
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            self.service = build('drive', 'v3', credentials=creds)
            print("Authentication successful.")
        except Exception as e:
            print(f"Authentication error: {e}")
            raise

    def get_folder_id(self, folder_name):
        """Get folder ID by name within the parent folder."""
        try:
            query = (f"name='{folder_name}' and "
                     f"'{self.parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")
            
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            files = results.get('files', [])
            if files:
                print(f"Folder '{folder_name}' found with ID: {files[0]['id']}")
                return files[0]['id']
            else:
                print(f"Folder '{folder_name}' does not exist.")
                return None
        except Exception as e:
            print(f"Error getting folder ID: {e}")
            return None

    def create_folder(self, folder_name):
        try:
            print(f"Creating folder '{folder_name}'...")
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.parent_folder_id]
            }
            # Add permission check
            self.service.files().get(fileId=self.parent_folder_id).execute()
        
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
        
            # Verify folder creation
            created_folder = self.service.files().get(
                fileId=folder.get('id'),
                fields='id, name'
            ).execute()
        
            print(f"Folder '{folder_name}' created and verified with ID: {folder.get('id')}")
            return folder.get('id')
        except Exception as e:
            print(f"Error creating folder: {e}")
            raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive."""
        try:
            print(f"Uploading file: {file_name}")
            file_metadata = {
                'name': os.path.basename(file_name),
                'parents': [folder_id]
            }
            media = MediaFileUpload(file_name, resumable=True)
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"File '{file_name}' uploaded with ID: {file.get('id')}")
            return file.get('id')
        except Exception as e:
            print(f"Error uploading file: {e}")
            raise

    def save_files(self, files):
        """Save files to Google Drive in a folder named after yesterday's date."""
        try:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            folder_id = self.get_folder_id(yesterday)
            if not folder_id:
                folder_id = self.create_folder(yesterday)
            
            for file_name in files:
                self.upload_file(file_name, folder_id)
            
            print(f"All files uploaded successfully to Google Drive folder '{yesterday}'.")
        except Exception as e:
            print(f"Error saving files: {e}")
            raise
