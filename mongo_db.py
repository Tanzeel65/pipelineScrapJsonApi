
import pymongo

url = 'mongodb://localhost:27017/'

client=pymongo.MongoClient(url)

db = client['C-DAC']

user_collection=db['Users']
data_collection=db['APIData']

flagged_data_collection=db['FlaggedData']

chart_collection=db['Analytics']

consolidated_collection=db['ConsolidatedData']

all_postId_collection=db['PostIds']

json_collection = db['JSONDetailes']

insta_collection=db['InstagramAPIData']
twitter_collection=db['TwitterAPIData']

app_link_colln=db['ApproverUploadColln']


pdf_colln=db['ReportPdfColln']


