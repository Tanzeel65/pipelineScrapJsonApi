import os
import random
import re
import string
from bson import ObjectId
import requests
from mongo_db import data_collection,chart_collection,json_collection
from pipelineScrapJsonApi.common_utils import file_status,file_extension

def process_single_document(docs, total_documents, mongoid):
    post_id = docs.get("postId", 0)

    existing_post = data_collection.find_one({"postId": post_id})

    if existing_post:
        return total_documents

    img_links = docs.get("attachedImageLinks", [])
    video_links = docs.get("attachedVideoLinks", [])
    sucessmlNewIds = []
    failedmlNewIds = []
    mlNewIds = []
    
    if len(img_links) > 0 or len(video_links) > 0:
        number = 0

        for media_link in img_links + video_links:
            downloaded_file, failure_reason = download_media(media_link, destination_folder="media")
            print('downlodfile', downloaded_file)
            print("failurereason", failure_reason)

            ext = os.path.splitext(downloaded_file)[1].lower() if downloaded_file else None

            if ext and ext in ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.avi', '.mov', '.mkv', '.mp3', '.wav', '.flac', '.ogg']:
                print('Uploading file to ML API...')
                unique_Id = f"{post_id}_{number}"
                print("postId", post_id)
                try:
                    status_code, ml_response = upload_to_ml_api(unique_Id, "Media", downloaded_file)
                    print('ML response:', ml_response)
                    print('ML response statuscode:', status_code)

                    if status_code == 200:
                        # changing status here only for one mediafile of postId
                        docs["Status"] = -1
                        mlNewId = ml_response.get('Unique ID', None)
                        mlNewIds.append(mlNewId)
                        docs["mlNewId"] = mlNewIds
                        sucessmlNewIds.append(mlNewId)
                        docs['sucessmlNewIds'] = sucessmlNewIds
                        break
           
                    if status_code == 409:
                        process_409_response(ml_response, docs, post_id, failure_reason, mongoid, sucessmlNewIds, failedmlNewIds, mlNewIds)

                    elif status_code == 400:
                        while True:  # Continue indefinitely until a response other than status code 400 is received
                            number += 1
                            unique_Id = f"{post_id}_{number}"
                            print('400res unique_Id', unique_Id)
                            status_code, ml_response = upload_to_ml_api(unique_Id, "Media", downloaded_file)
                            print('400res MLresponse:', ml_response)
                            print('400res MLresponse statuscode:', status_code)

                            if status_code == 200:
                                # postId have one media and they have,uniquealreadyexist,or data alreadyexis
                                docs["Status"] = -1
                                mlNewId = ml_response.get('Unique ID', None)
                                mlNewIds.append(mlNewId)
                                docs["mlNewId"] = mlNewIds
                                sucessmlNewIds.append(mlNewId)
                                docs['sucessmlNewIds'] = sucessmlNewIds
                                break

                            if status_code == 409:
                                process_409_response(ml_response, docs, post_id, failure_reason, mongoid, sucessmlNewIds, failedmlNewIds, mlNewIds)
                                break
                            
                except requests.exceptions.Timeout:
                    print("Timeout occurred during the ML API call. Treating as a 500 response.")
                    return 'Timeout occurred', 500

                except Exception as e:
                    print(f"Error during ML API call: {e}")

            else: 
                # below if else for when filedownloaded but they are zip or other,handled failure reason manually 
                if not ext:
                     failure_reason = failure_reason
                else:
                    failure_reason = f"{ext} file extension is not allowed to upload"
                
                # Add failed media information to the list
                json_collection.update_one({"_id": ObjectId(mongoid)}, {"$push": {
                    "failedMediaPostid": {"postId": post_id, "reason": failure_reason}}})
                json_collection.update_one({"_id": ObjectId(mongoid)}, {"$inc": {"failedMLExtractionCount": 1}})


        if len(sucessmlNewIds) > 0:
            docs["Status"] = -1
        elif len(sucessmlNewIds) == 0:
            docs["Status"] = -2

        data_collection.insert_one(docs)
        print("one docs inserted")
        json_collection.update_one({"_id": ObjectId(mongoid)}, {"$push": {"mediaPostId": post_id}})
        json_collection.update_one({"_id": ObjectId(mongoid)}, {"$inc": {"mediaCount": 1}})
 
        # post_ids_with_media.append(post_id)
    else:

        docs["Status"] = 0  
        data_collection.insert_one(docs)
        print("one nomediapostdocs inserted")
        json_collection.update_one({"_id": ObjectId(mongoid)}, {"$push": {"noMediaPostid": post_id}})

    total_documents += 1

    return total_documents


def process_409_response(ml_response, docs, post_id, failure_reason,mongoid,sucessmlNewIds,failedmlNewIds,mlNewIds):
    mlNewId = ml_response.get('uniqueId', None)
    
    mlNewIds.append(mlNewId)
    docs["mlNewId"] = mlNewIds
    print('docs["mlNewId"]', docs["mlNewId"])

    file_status_response, response = file_status(mlNewId)
    print('File status response:', file_status_response)
    print('File status API response:', response)

    if file_status_response == 200:
        sucessmlNewIds.append(mlNewId)
        docs['sucessmlNewIds'] =sucessmlNewIds
        print("successmlNewId",sucessmlNewIds)

    else:
        failedmlNewIds.append(mlNewId)
        docs['failedmlNewIds'] = failedmlNewIds

        # failed_media_info.append(handle_failed_media(post_id, failure_reason))
        json_collection.update_one({"_id": ObjectId(mongoid)},{
        "$set": {'failedMediaPostid': [{'postId': post_id, 'reason': failure_reason}]},
        "$inc": {"failedMLExtractionCount": 1}})




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
            print('error meaage',error_message)
            return None, error_message
    else:
        error_message = f"Failed to download media from {url}. Status code: {response.status_code}"
        print('error meaage',error_message)

        return None, error_message
 
def sanitize_file_name(file_name):
    try:
        # Extract filename without extension
        base_name = os.path.splitext(file_name)[0]
        
        # If base_name is empty after removing extension or only contains special characters,
        # generate a random filename
        if not base_name or not re.match(r'^[a-zA-Z0-9_\.]+$', base_name):
            base_name = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        
        # Replace invalid characters with underscores
        sanitized_name = re.sub(r'[\/:*?"<>|]', '_', base_name)
        
        # Ensure filename is not empty
        if not sanitized_name:
            sanitized_name = 'unnamed_file'
        
        # Reconstruct filename with extension
        sanitized_file_name = sanitized_name + os.path.splitext(file_name)[1]

        return sanitized_file_name
    except Exception as e:
        print(f"An error occurred while sanitizing the file name: {e}")
        return None

# Function for uploading file to upload_to_ml_api
def upload_to_ml_api(unique_id,description,file_path):
    
    print('uniqueid',unique_id)
    # print('descripton',description)
    print('filepath',file_path)

    url = "http://49.249.168.190:7000/upload"
    
    json_data = {
        "unique_id": unique_id,  # Use unique_id instead of post_id
        "description": description
    }
    
    files = {'file': open(file_path, 'rb')}
    
    response = requests.post(url, data=json_data, files=files)

    return response.status_code, response.json()
