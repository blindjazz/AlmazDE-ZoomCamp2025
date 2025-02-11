import os
import sys
from google.cloud import storage

print(sys.executable)

def implicit():
    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    project = 'kestra-zoomcamp-449902'
   
    CREDENTIALS_FILE = "/Users/almaz/Documents/jobs/git/gcp_keys/ny_rides.json"  
    client = storage.Client.from_service_account_json(CREDENTIALS_FILE)

    storage_client = storage.Client(project=project)

    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    print(buckets)

implicit()