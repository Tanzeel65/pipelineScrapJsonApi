import json
import threading
import time
from bson import ObjectId
import pandas as pd
import logging
from flask import Blueprint, jsonify, request
import requests
# from Routes.Pipelines.periodiccheck_ml_status import start_check_ml_status_thread
from mongo_db import data_collection,chart_collection,json_collection
import pprint
import os
from datetime import datetime
# from Code_Base.APIData_post import data_upload, update_platformwise
from pipelineScrapJsonApi.periodiccheck_ml_status import start_check_ml_status_thread
from pipelineScrapJsonApi.process_single_document import process_single_document
# logger = configure_logger("json_upload_route",'logs/json_upload_route.log', logging.INFO)
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
        process_json_documents(df,mongo_id)
        


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



def process_json_documents(df, mongoid):
    total_documents = 0
   

    for docs in df.to_dict(orient='records'):
        total_documents = process_single_document(docs, total_documents, mongoid)


    json_doc = json_collection.find_one_and_update(
        {"_id": ObjectId(mongoid)},
        {"$set": {
            "totalJsonCount": total_documents,
            
        }},
        return_document=True
    )

    if json_doc:
        print(f"JSON document with mongoid {mongoid} updated successfully.")
    else:
        print(f"No matching JSON document found for mongoid {mongoid}.")



start_check_ml_status_thread()





