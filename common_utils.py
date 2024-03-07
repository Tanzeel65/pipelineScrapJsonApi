
import requests


postId_to_mlNewId_mapping = {}


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
