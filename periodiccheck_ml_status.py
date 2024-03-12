
import threading
import time
from mongo_db import data_collection, json_collection
from pipelineScrapJsonApi.common_utils import file_status 



# Function to periodically check ML status API and update MongoDB
def check_ml_status():
    print("Thread started successfully.")
    
    while True:
        try:
            posts_to_check = data_collection.find({"Status": -1})
            print("post to check", posts_to_check)

            for post in posts_to_check:
                post_id = post.get("postId")
                print('post id', post_id)

                # Get the mlNewId array using the mapping
                ml_new_id_arr = data_collection.find_one({"postId": post_id}, {"mlNewId": 1, "_id": 0})
                print('ml new id array', ml_new_id_arr)

                # Correct the loop to iterate over the array
                for ml_new_id in ml_new_id_arr.get("mlNewId", []):
                    print('ml new id', ml_new_id)

                    if ml_new_id:
                        status_code, responsedata = file_status(ml_new_id)
                        print('periodically status', status_code)

                        if status_code == 200 and responsedata:
                            # Merge "Status": 0 and responsedata in the update query
                            update_fields = {"Status": 0, **responsedata}
                            # Update the document in data_collection with the merged fields
                            data_collection.update_one({"postId": post_id}, {"$set": update_fields})

                        elif status_code == 404:
                            # Post ID not found, update MongoDB with status=-2 and increment failed ML extraction count
                            data_collection.update_one(
                            {"postId": post_id},
                            {"$set": {"Status": -2}, "$inc": {"failedMLExtractionCount": 1}})

                        elif status_code == 500:
                            api_data = data_collection.find_one({"postId": post_id})
                            if api_data:
                                data_collection.update_one({"postId": post_id},
                               {"$set": {"Status": -1}, "$inc": {"count": 1}})

                            if api_data.get("count", 0) == 5:
                              data_collection.update_one({"postId": post_id}, {"$set": {"Status": -2}})
                              json_collection.update_one({"postId": post_id}, {"$inc": {"failedMLExtractionCount": 1}})
                           
                        else:
                            # Handle other status codes as needed
                            pass

            time.sleep(60)  # Adjust the interval as needed (in seconds)

        except Exception as e:
            print(f"Error in check_ml_status: {str(e)}")


def start_check_ml_status_thread():
    ml_status_thread = threading.Thread(target=check_ml_status)
    ml_status_thread.start()

