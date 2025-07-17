# Import required modules
import os  # For file path operations
import json  # For handling JSON data
from google.oauth2.service_account import Credentials  # For Google API authentication using service accounts
from googleapiclient.discovery import build  # To build the Google Drive API service
from googleapiclient.http import MediaFileUpload  # To handle file upload to Google Drive
from datetime import datetime, timedelta  # For handling dates

# Define a class to handle saving files to Google Drive under a specific folder
class SavingOnDriveServices:
    def __init__(self, credentials_dict):
        # Initialize with credentials dictionary provided from environment or secure storage
        self.credentials_dict = credentials_dict
        
        # Define the required scope for accessing Google Drive
        self.scopes = ['https://www.googleapis.com/auth/drive']
        
        # This will hold the authenticated Google Drive service object
        self.service = None
        
        # ID of the parent folder in Google Drive where all subfolders/files will be created/uploaded
        self.parent_folder_id = '15Ggg_hhXLM4C4LUNiyg13IP4VRMFcjUN'

    def authenticate(self):
        """Authenticate with Google Drive API using service account credentials."""
        try:
            print("Authenticating with Google Drive...")
            
            # Load credentials from the provided service account dictionary and apply the scopes
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            
            # Build the Drive API client using the credentials
            self.service = build('drive', 'v3', credentials=creds)
            print("Authentication successful.")
        except Exception as e:
            # Handle and raise any authentication errors
            print(f"Authentication error: {e}")
            raise

    def get_folder_id(self, folder_name):
        """Get the ID of a folder with the given name under the specified parent folder."""
        try:
            # Construct query to find folder by name under the parent folder and not trashed
            query = (f"name='{folder_name}' and "
                     f"'{self.parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")
            
            # Execute the query to search for the folder
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'  # Only return id and name fields
            ).execute()
            
            # Get the list of files (folders)
            files = results.get('files', [])
            if files:
                # Folder exists, return its ID
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
        """Create a new folder under the parent folder and return its ID."""
        try:
            print(f"Creating folder '{folder_name}'...")
            
            # Define the metadata for the folder to be created
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',  # Specifies it's a folder
                'parents': [self.parent_folder_id]  # Place it under the parent folder
            }
            
            # Call the Drive API to create the folder
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'  # Only retrieve the ID of the newly created folder
            ).execute()
            
            # Log and return the new folder's ID
            print(f"Folder '{folder_name}' created with ID: {folder.get('id')}")
            return folder.get('id')
        except Exception as e:
            # Handle any error during folder creation
            print(f"Error creating folder: {e}")
            raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to the specified folder in Google Drive."""
        try:
            print(f"Uploading file: {file_name}")
            
            # Prepare the metadata for the file including its name and parent folder
            file_metadata = {
                'name': os.path.basename(file_name),  # Use only the file name (no path)
                'parents': [folder_id]  # Upload to the given folder
            }
            
            # Prepare the media (file content) to be uploaded
            media = MediaFileUpload(file_name, resumable=True)
            
            # Upload the file to Drive
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'  # Only return the file ID
            ).execute()
            
            # Log the successful upload
            print(f"File '{file_name}' uploaded with ID: {file.get('id')}")
            return file.get('id')
        except Exception as e:
            # Handle any error during file upload
            print(f"Error uploading file: {e}")
            raise

    def save_files(self, files):
        """Save a list of files to Google Drive in a folder named after yesterday's date."""
        try:
            # Calculate yesterday's date and format it as 'YYYY-MM-DD'
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            # Check if a folder for yesterday already exists
            folder_id = self.get_folder_id(yesterday)
            
            # If not, create the folder
            if not folder_id:
                folder_id = self.create_folder(yesterday)
            
            # Upload each file in the list to the determined folder
            for file_name in files:
                self.upload_file(file_name, folder_id)
            
            # Log final success message
            print(f"All files uploaded successfully to Google Drive folder '{yesterday}'.")
        except Exception as e:
            # Handle any error during the process
            print(f"Error saving files: {e}")
            raise
