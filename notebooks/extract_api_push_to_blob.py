import requests
import pandas as pd
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient
import io


# Calculate date range
today = datetime.today().date()
start_date = today - timedelta(days=7)
    
# API URL
url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_date}&end_date={today}&api_key=DEMO_KEY"
    
try:
    # Make the API request
    response = requests.get(url)
       
    if response.status_code == 200:
        print("Success! Data retrieved")
            
        output = response.json()
            
        # Extract "near_earth_objects" data from nested json
        neo = output['near_earth_objects']
            
        # Create empty list to store appended data
        neo_data = []
            
        # extract value data from each date, assign the date to each value and append to the empty list
        for date, neos in neo.items():
            for neo_object in neos:
                neo_object['date'] = date
                neo_data.append(neo_object)

        # Create df
        df = pd.DataFrame(neo_data)
        
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
except Exception as e:
    print(f"An error occurred: {e}")
    
raw_df = df

# Import data to blob storage
from azure.storage.blob import BlobServiceClient

# Replace with your actual connection string
connection_string = "***"

container_name = "bronze"
blob_name = f"raw_neo_data_{today}.csv"  # Assuming 'today' is defined elsewhere

#rename csv for clarity
bronze_df_to_csv = raw_df.to_csv(index=False)

# encode data to match blob storage specs
csv_bronze_df_bytes = bronze_df_to_csv.encode('utf-8')

try:
    # Create a BlobServiceClient object and reference the container to import to
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)


    # Upload the data to blob storage
    with io.BytesIO(csv_bronze_df_bytes) as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)

    print(f"File uploaded to {container_name}/{blob_name}")

except Exception as e:
    print(f"An error occurred: {e}")