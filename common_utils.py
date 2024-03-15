
import requests



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


def file_extension(filename):
    filename_parts = str(filename).split('.')
    if len(filename_parts) > 1:
        file_extension = filename_parts[-1]
        return str(file_extension)
    else:
        raise ValueError("Invalid file extension.")