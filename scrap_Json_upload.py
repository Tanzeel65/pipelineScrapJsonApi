import json
import threading
import time
from bson import ObjectId
import pandas as pd
import logging
from flask import Blueprint, jsonify, request
import requests
from Code_Base.logging import configure_logger
from mongo_db import data_collection,chart_collection,json_collection
import pprint
import os
from datetime import datetime
from Code_Base.APIData_post import data_upload, update_platformwise
logger = configure_logger("json_upload_route",'logs/json_upload_route.log', logging.INFO)
import re


"""
    - when we got json
    - upload in mongo with JSONColln 
                    - time    --timeOfCreation
                    - total COUNT OF DOC   --totalJsonCount  int
                    - media count uploaded to ml  --mediaCount   int
                    - post_id who has media     --mediaPostId--(objectID)
                    - postid dont have media   --noMediaPostid---(objectID)
                    - failed ml extraction count   --- int
                    - failed ml extraction postid    -- failedMediaPostid---[{postId(objectID)},{reason}]

    - json file have save in local with name has to be _id of mongo
    - Upload each document to mongo APIdata coln and put status=-1 
    check postid alredy exists or not it has
      no media put status = 0
    - count the doc and upload that in json mongo coln
    - download the media and upload to ml with postid in the object  
      (should i save , media will large size of server )
    - count the media uplodes for that json upload in mongo
               - 200 --> ok
               - 409 --> media already there response will return new post id 
               check status api if its 200 update the 
                         apidata coln else continue and update that post id to
                           -2 in apidata coln and failed ml extraction count +1 
                         update failed ml extraction postid 
               - 400 --> id is duplicate check the status with this postid  and 
               update apidata coln     


    - hit the ml status api with postid whose status=-1 in reguler interval
              - 200--> update the mongo with the data status=0  update all fields 
              - 401 -->not finished 
              - 404 --> post id not found update mongo with status =-2 
              failed ml extraction count +1
              - 500 --> for 5 time take the object from API data coln and 
                        add failed ml coln  update mongo with status =-2 
                        failed ml extraction count +1
    ??
    # - what will do one postid have two media 
     
"""


json_upload_routess = Blueprint('json_upload_routess', __name__)
@json_upload_routess.route('', methods=['POST'])
def json_upload():
    try:
        if 'json' not in request.files:
            raise ValueError("No JSON file provided")

        json_file = request.files['json']

        ext = file_extension(json_file.filename)
        if ext not in ['json', 'JSON']:
            raise ValueError("Invalid file type. Please upload a JSON file.")

        time_of_creation = datetime.utcnow()
        inserted_document = json_collection.insert_one({"timeOfCreation": time_of_creation})
        mongo_id = inserted_document.inserted_id

        print('mongoid', mongo_id)
        upload_file(json_file,mongo_id)

        json_data = json.load(open(f'Uploads/json/{mongo_id}.json', encoding='utf-8'))
        df = pd.DataFrame(json_data)
        process_documents(df,mongo_id)


        return jsonify({"message": "File uploaded successfully"}), 200

    except ValueError as e:
        return jsonify({"message": str(e)}), 400
    except FileNotFoundError as e:
        return jsonify({"message": str(e)}), 404
    except FileExistsError as e:
        return jsonify({"message": str(e)}), 409
    except Exception as e:
        # Log the error for debugging
        print(f"Error: {str(e)}")
        return jsonify({"message": "Server error"}), 500

# Function for uploading jsonfile
def upload_file(file, file_name):
    try:
        file.save(f'Uploads/json/{file_name}.json')
    except Exception as e:
        raise ValueError("Error saving file.")


def file_extension(filename):
    filename_parts = str(filename).split('.')
    if len(filename_parts) > 1:
        file_extension = filename_parts[-1]
        return str(file_extension)
    else:
        raise ValueError("Invalid file extension.")


# Function for sanitize the filename which is extracted from mediaurl
def sanitize_file_name(file_name):
    # Replace invalid characters with underscores
    sanitized_name = re.sub(r'[\/:*?"<>|]', '_', file_name)
    # Remove any remaining non-alphanumeric characters
    sanitized_name = re.sub(r'[^a-zA-Z0-9_\.]', '', sanitized_name)
    return sanitized_name


# Function for download media from jsonfile medialink
def download_media(url, destination_folder):
    response = requests.get(url)
    
    if response.status_code == 200:
        # Create the destination folder if it doesn't exist
        os.makedirs(destination_folder, exist_ok=True)

        # Extract the file name from the URL and sanitize it
        sanitized_file_name = sanitize_file_name(os.path.basename(url))
        file_path = os.path.join(destination_folder, sanitized_file_name)

        try:
            with open(file_path, 'wb') as file:
                file.write(response.content)
            return file_path, None  # Successful download, return file path and no failure reason
        except Exception as e:
            error_message = f"Failed to save media to {file_path}. Error: {e}"
            # print(error_message)
            return None, error_message
    else:
        error_message = f"Failed to download media from {url}. Status code: {response.status_code}"
        # print(error_message)
        return None, error_message


# Function for handle_failed_media
def handle_failed_media(post_id, failure_reason):
    # print(f"Handling failed media for post ID {post_id}. Reason: {failure_reason}")

    return post_id, failure_reason


# Function for uploading file to upload_to_ml_api
def upload_to_ml_api(unique_id,description,file_path):
    # file_path = 'media\\cs.png'  # Removed the trailing comma
    # unique_id = '65d5a9e4fa672b2e4'  # Removed the trailing comma
    # description = 'Media'
    print('uniqueid',unique_id)
    print('descripton',description)
    print('filepath',file_path)
    

    
    url = "http://49.249.168.190:7000/upload"
    
    json_data = {
        "unique_id": unique_id,  # Use unique_id instead of post_id
        "description": description
    }
    
    files = {'file': open(file_path, 'rb')}
    
    response = requests.post(url, data=json_data, files=files)

    return response.status_code, response.json()

# print(upload_to_ml_api())

# Function for getting file_status using mlNewId
def file_status(mlNewId):
    print(mlNewId)

    url = f"http://49.249.168.190:7000/file_status/{mlNewId}"
    response = requests.get(url)

    if response.status_code == 200:
        print("Data sent successfully")

        return response.status_code,response.json()
    else:
        print("Failed to send data. Status code:", response.status_code)
        return f"failed to send data error: {response.status_code}"

postId_to_mlNewId_mapping={}

# Function for process jsonFile
def process_documents(df, mongoid):
    total_documents = 0
    post_ids_with_media = []
    post_ids_without_media = []
    failed_media_info = []  # List to store failed media information

    for docs in df.to_dict(orient='records'):
        post_id = docs.get("postId", 0)

        existing_post = data_collection.find_one({"postId": post_id})

        if not existing_post:
            docs["Status"] = 0
            img_links = docs.get("attachedImageLinks", [])
            video_links = docs.get("attachedVideoLinks", [])
            if len(img_links) > 0 or len(video_links) > 0:
                number = 0

                for media_link in img_links + video_links:
                    downloaded_file, failure_reason = download_media(media_link, destination_folder="media")

                    if downloaded_file:
                        print('Uploading file to ML API...')
                        unique_Id = f"{post_id}_{number}"
                        print('unique_Id', unique_Id)

                        try:
                            status_code, ml_response = upload_to_ml_api(unique_Id, "Media", downloaded_file)
                            print('ML response:', ml_response)

                            if status_code == 409:
                                mlNewId = ml_response.get('uniqueId', None)
                                print('ML new id:', mlNewId)

                                # storing mlNewId in postId_to_mlNewId_mapping so that we use in periodically check_ml_status func
                                postId_to_mlNewId_mapping[post_id] = mlNewId  # Store the mapping


                                file_status_response, response = file_status(mlNewId)
                                print('File status response:', file_status_response)
                                print('File status API response:', response)

                                if file_status_response == 200:
                                    docs["Status"] = -1
                                else:
                                    docs["Status"] = -2
                                    failed_media_info.append(handle_failed_media(post_id, failure_reason))
                            else:
                                print(f"Unexpected status code from ML API: {status_code}")
                        except Exception as e:
                            print(f"Error during ML API call: {e}")

                        number += 1
                    else:
                        # Add failed media information to the list
                        failed_media_info.append(handle_failed_media(post_id, failure_reason))

                data_collection.insert_one(docs)
                post_ids_with_media.append(post_id)
            else:
                post_ids_without_media.append(post_id)

            total_documents += 1
        else:
            continue

    failed_ml_count = len(failed_media_info)

    json_doc = json_collection.find_one_and_update(
        {"_id": ObjectId(mongoid)},
        {"$set": {
            "totalJsonCount": total_documents,
            "mediaCount": len(post_ids_with_media),
            "mediaPostId": post_ids_with_media,
            "noMediaPostid": post_ids_without_media,
            "failedMLExtractionCount": failed_ml_count,
            "failedMediaPostid": failed_media_info
        }},
        return_document=True
    )

    if json_doc:
        print(f"JSON document with mongoid {mongoid} updated successfully.")
    else:
        print(f"No matching JSON document found for mongoid {mongoid}.")



# Function to periodically check ML status API and update MongoDB
def check_ml_status():
    print("Thread started successfully.")

    while True:
        try:
            posts_to_check = data_collection.find({"Status": -1})

            for post in posts_to_check:
                post_id = post.get("postId")
                retry_count = 0
                 # Get the mlNewId using the mapping
                mlNewId = postId_to_mlNewId_mapping.get(post_id)


                status_code, responsedata = file_status(mlNewId)
                print('periodically status', status_code)

                if status_code == 200 and responsedata:
                    # Merge "Status": 0 and responsedata in the update query
                    update_fields = {"Status": 0, **responsedata}
                    # Update the document in data_collection with the merged fields
                    data_collection.update_one({"postId": post_id}, {"$set": update_fields})
                    break
                elif status_code == 404:
                    # Post ID not found, update MongoDB with status=-2 and increment failed ML extraction count
                    data_collection.update_one({"postId": post_id}, {"$set": {"Status": -2},
                                                    "$inc": {"failedMLExtractionCount": 1}})
                    break
                elif status_code == 500:
                    while retry_count < 5:
                        api_data = data_collection.find_one({"postId": post_id})
                        if api_data:
                            json_collection.update_one({"postId": post_id}, {"$inc": {"failedMLExtractionCount": 1}})
                            data_collection.update_one({"postId": post_id}, {"$set": {"Status": -2}})
                            break

                        # Retry for a few times to handle 500 errors
                        retry_count += 1
                        time.sleep(1)  # Wait for 1 second before retrying

                    # Wait for 1 second before retrying
                    else:
                        # Handle other status codes as needed
                        break

            time.sleep(60)  # Adjust the interval as needed (in seconds)

        except Exception as e:

            print(f"Error in check_ml_status: {str(e)}")


ml_status_thread = threading.Thread(target=check_ml_status)
ml_status_thread.start()
