import azure.functions as func
import logging
import json
import binascii
import os
import snowflake.connector
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="raw/{name}",
                  connection="AzureWebJobsStorage") 
def ProcessGuestImages(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processing blob: {myblob.name}")
    
    # helper function to get env vars safely
    def get_env_var(var_name):
        return os.environ.get(var_name)

    conn = None
    try:
        # 1. Connect to Azure Storage
        connect_str = get_env_var("AzureWebJobsStorage")
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        target_container = "image"
        container_client = blob_service_client.get_container_client(target_container)
        
        if not container_client.exists():
            container_client.create_container()

        # 2. Connect to Snowflake
        # We open the connection once per file execution to save time
        conn = snowflake.connector.connect(
            user=get_env_var("SNOWFLAKE_USER"),
            password=get_env_var("SNOWFLAKE_PASSWORD"),
            account=get_env_var("SNOWFLAKE_ACCOUNT"),
            warehouse=get_env_var("SNOWFLAKE_WAREHOUSE"),
            database=get_env_var("SNOWFLAKE_DB"),
            schema=get_env_var("SNOWFLAKE_SCHEMA")
        )
        cursor = conn.cursor()

        # 3. Read & Process File
        file_content = myblob.read().decode('utf-8')
        
        for line in file_content.splitlines():
            if not line.strip(): continue 
            
            try:
                json_obj = json.loads(line)
                
                # Check Requirement: ImageType must be 3
                if json_obj.get("ImageType") == 3:
                    
                    # --- A. Upload Image to Blob Storage ---
                    trans_id = str(json_obj.get("TransID"))
                    hex_string = json_obj.get("PlayerImage")
                    
                    if trans_id and hex_string:
                        image_binary = binascii.unhexlify(hex_string)
                        blob_name = f"{trans_id}.jpg"
                        blob_client = container_client.get_blob_client(blob_name)
                        blob_client.upload_blob(image_binary, overwrite=True)
                        
                        # Construct the URL for the new image
                        # Format: https://<account>.blob.core.windows.net/<container>/<blob>
                        account_name = blob_service_client.account_name
                        image_url = f"https://{account_name}.blob.core.windows.net/{target_container}/{blob_name}"
                        
                        logging.info(f"Image Uploaded: {image_url}")

                        # --- B. Insert Metadata into Snowflake ---
                        # Prepare the SQL query
                        insert_query = """
                        INSERT INTO METADATA (
                            TransID, Date, PlayerID, ImageType, Status, SubTypeID, 
                            UserID, DisplayDefault, FromInterface, QLK_CHANGE_SEQ, 
                            QLK_CHANGE_OP, QLK_LOAD_TIMESTAMP, PlayerImageURL
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """

                        # Map JSON fields to SQL params
                        # Use .get() to handle missing keys gracefully (defaults to None)
                        params = (
                            json_obj.get("TransID"),
                            json_obj.get("Date"), # Ensure this matches Snowflake TIMESTAMP format
                            json_obj.get("PlayerID"),
                            json_obj.get("ImageType"),
                            json_obj.get("Status"),
                            json_obj.get("SubTypeID"),
                            json_obj.get("UserID"),
                            json_obj.get("DisplayDefault"),
                            json_obj.get("FromInterface"),
                            json_obj.get("QLK_CHANGE_SEQ"),
                            json_obj.get("QLK_CHANGE_OP"),
                            json_obj.get("QLK_LOAD_TIMESTAMP"),
                            image_url  # Our calculated URL
                        )

                        cursor.execute(insert_query, params)
                        logging.info(f"Snowflake Insert Success for TransID: {trans_id}")

            except Exception as row_error:
                logging.error(f"Error processing row: {row_error}")

    except Exception as e:
        logging.error(f"Global Error: {e}")
    finally:
        # Always close the Snowflake connection
        if conn:
            cursor.close()
            conn.close()