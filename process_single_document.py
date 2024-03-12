import os
import random
import re
import requests
# from Routes.Pipelines.scrap_Json_upload import download_media, handle_failed_media, upload_to_ml_api
from mongo_db import data_collection,chart_collection,json_collection
from pipelineScrapJsonApi.common_utils import file_status

def process_single_document(docs, total_documents, 
                            failed_media_info, post_ids_with_media, post_ids_without_media):
    post_id = docs.get("postId", 0)

    existing_post = data_collection.find_one({"postId": post_id})

    if existing_post:
        return total_documents

    docs["Status"] = 0
    img_links = docs.get("attachedImageLinks", [])
    video_links = docs.get("attachedVideoLinks", [])
    mlNewIds = []  # Create an empty array to store mlNewIds

    if len(img_links) > 0 or len(video_links) > 0:
        number = 0

        for media_link in img_links + video_links:
            downloaded_file, failure_reason = download_media(media_link, destination_folder="media")

            if downloaded_file:
                print('Uploading file to ML API...')

                unique_Id = f"{post_id}_{number}"
                print('unique_Idint', unique_Id)

                try:
                    status_code, ml_response = upload_to_ml_api(unique_Id, "Media", downloaded_file)
                    print('ML response:', ml_response)
                    print('ML response statuscode:', status_code)

                    if status_code == 409:
                        process_409_response(ml_response, docs, mlNewIds, post_id, failure_reason,failed_media_info)

                        
                    elif status_code == 400:
                        max_attempts = 6  # Set your desired maximum attempts
                        attempts = 0
                        while attempts < max_attempts:
                            number += 1

                            unique_Id = f"{post_id}_{number}"
                            print('400res unique_Id', unique_Id)
                            status_code, ml_response = upload_to_ml_api(unique_Id, "Media", downloaded_file)
                            print('400res MLresponse:', ml_response)
                            print('400res MLresponse statuscode:', status_code)

                            if status_code == 400:
                                attempts += 1
                            else:
                                process_409_response(ml_response, docs, mlNewIds, post_id, failure_reason,failed_media_info)


                                # mlNewId = ml_response.get('uniqueId', None)
                                # print('ML new id:', mlNewId)

                                # # storing mlNewId in postId_to_mlNewId_mapping so that we use in periodically check_ml_status func
                                # mlNewIds.append(mlNewId)
                                # docs["mlNewId"] = mlNewIds
                                # print('docs["mlNewId"]', docs["mlNewId"]) 

                                # file_status_response, response = file_status(mlNewId)
                                # print('File status response:', file_status_response)
                                # print('File status API response:', response)

                                # if file_status_response == 200:
                                #     docs["Status"] = -1
                                # else:
                                #     docs["Status"] = -2
                                #     failed_media_info.append(handle_failed_media(post_id, failure_reason))
                                break  # Exit the while loop if upload is successful or a different error occurs
                       
                except requests.exceptions.Timeout:
                    print("Timeout occurred during the ML API call. Treating as a 500 response.")
                    return 'Timeout occurred', 500

                except Exception as e:
                    print(f"Error during ML API call: {e}")

                number += 1
            else:
                # Add failed media information to the list
                failed_media_info.append(handle_failed_media(post_id, failure_reason))

        data_collection.insert_one(docs)
        print("one docs inserted")
        post_ids_with_media.append(post_id)
    else:
        post_ids_without_media.append(post_id)

    total_documents += 1

    return total_documents


def process_409_response(ml_response, docs, mlNewIds, post_id, failure_reason,failed_media_info):
    mlNewId = ml_response.get('uniqueId', None)
    print('ML new id:', mlNewId)

    # storing mlNewId in postId_to_mlNewId_mapping so that we use in periodically check_ml_status func
    mlNewIds.append(mlNewId)
    docs["mlNewId"] = mlNewIds
    print('docs["mlNewId"]', docs["mlNewId"])

    file_status_response, response = file_status(mlNewId)
    print('File status response:', file_status_response)
    print('File status API response:', response)

    if file_status_response == 200:
        docs["Status"] = -1
    else:
        docs["Status"] = -2
        failed_media_info.append(handle_failed_media(post_id, failure_reason))




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


# Function for sanitize the filename which is extracted from mediaurl
def sanitize_file_name(file_name):
    # Replace invalid characters with underscores
    sanitized_name = re.sub(r'[\/:*?"<>|]', '_', file_name)
    # Remove any remaining non-alphanumeric characters
    sanitized_name = re.sub(r'[^a-zA-Z0-9_\.]', '', sanitized_name)
    return sanitized_name


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
