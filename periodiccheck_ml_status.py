
import threading
import time
from Routes.Pipelines.common_utils import file_status,postId_to_mlNewId_mapping
from mongo_db import data_collection, json_collection 



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

                        retry_count += 1
                        time.sleep(1)  # Wait for 1 second before retrying

                    else:
                        # Handle other status codes as needed
                        break

            time.sleep(20)  # Adjust the interval as needed (in seconds)

        except Exception as e:

            print(f"Error in check_ml_status: {str(e)}")


def start_check_ml_status_thread():
    ml_status_thread = threading.Thread(target=check_ml_status)
    ml_status_thread.start()

