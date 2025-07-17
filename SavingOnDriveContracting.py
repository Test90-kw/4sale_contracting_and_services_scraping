import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime, timedelta

class SavingOnDriveContracting:
    def __init__(self, credentials_dict):
        # Store the provided credentials dictionary for authentication
        self.credentials_dict = credentials_dict
        # Define the required Google Drive API scope
        self.scopes = ['https://www.googleapis.com/auth/drive']
        # Will hold the authenticated Google Drive service instance
        self.service = None
        # Google Drive folder ID where subfolders will be created
        self.parent_folder_id = '1pMrJF8bVTJIxurHLUG_g-oviK5sH27gY'

    def authenticate(self):
        """Authenticate with Google Drive API."""
        try:
            print("Authenticating with Google Drive...")
            # Create credentials from the service account info and scopes
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            # Build the Google Drive service object
            self.service = build('drive', 'v3', credentials=creds)
            print("Authentication successful.")
        except Exception as e:
            # Print and raise an error if authentication fails
            print(f"Authentication error: {e}")
            raise

    def get_folder_id(self, folder_name):
        """Get folder ID by name within the parent folder."""
        try:
            # Query to find a folder with the specified name under the parent folder
            query = (f"name='{folder_name}' and "
                     f"'{self.parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")
            
            # Execute the query and fetch matching folders
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()
            
            # Extract the list of files from the result
            files = results.get('files', [])
            if files:
                # If folder exists, return its ID
                print(f"Folder '{folder_name}' found with ID: {files[0]['id']}")
                return files[0]['id']
            else:
                # Folder not found
                print(f"Folder '{folder_name}' does not exist.")
                return None
        except Exception as e:
            # Handle any error during folder search
            print(f"Error getting folder ID: {e}")
            return None

    def create_folder(self, folder_name):
        """Create a new folder in the parent folder."""
        try:
            print(f"Creating folder '{folder_name}'...")
            # Define metadata for the new folder
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.parent_folder_id]
            }
            # Create the folder using the Drive API
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            # Return the newly created folder ID
            print(f"Folder '{folder_name}' created with ID: {folder.get('id')}")
            return folder.get('id')
        except Exception as e:
            # Handle folder creation errors
            print(f"Error creating folder: {e}")
            raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive."""
        try:
            print(f"Uploading file: {file_name}")
            # Set the file metadata including target folder
            file_metadata = {
                'name': os.path.basename(file_name),
                'parents': [folder_id]
            }
            # Prepare the file for upload
            media = MediaFileUpload(file_name, resumable=True)
            # Upload the file using the Drive API
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            # Return the uploaded file's ID
            print(f"File '{file_name}' uploaded with ID: {file.get('id')}")
            return file.get('id')
        except Exception as e:
            # Handle upload errors
            print(f"Error uploading file: {e}")
            raise

    def save_files(self, files):
        """Save files to Google Drive in a folder named after yesterday's date."""
        try:
            # Get yesterday's date as a string in YYYY-MM-DD format
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            # Try to get the folder ID if it already exists
            folder_id = self.get_folder_id(yesterday)
            if not folder_id:
                # Create the folder if it doesn't exist
                folder_id = self.create_folder(yesterday)
            
            # Upload each file in the provided list to the folder
            for file_name in files:
                self.upload_file(file_name, folder_id)
            
            # Confirm successful upload
            print(f"All files uploaded successfully to Google Drive folder '{yesterday}'.")
        except Exception as e:
            # Handle any errors during the save process
            print(f"Error saving files: {e}")
            raise
