import threading
from flask import Flask
from flask_cors import CORS
from Routes.Logiin_Management.login_route import login_route, signup_route
from Routes.Dashboard.dashboard_route import dashboard_data_route
from Routes.Flag_Management.post_flag_route import flag_create_route
from Routes.Flag_Management.user_flag_route import user_flag_create_route
from Routes.Flag_Management.reject_flag_route import flag_reject_route
from Routes.Dashboard.approved_dashboard import approved_dashboard_data_route
from Routes.Dashboard.reject_dashboard import Reject_dashboard_data_route
from Routes.Dashboard.get_analytics import analatics_data_route
from Routes.Dashboard.json_upload import json_upload_route
from Routes.Dashboard.download_pdf import pdf_download_route, pdf_download_all_route
from Routes.Dashboard.search_route import data_search_route
from Routes.Logiin_Management.change_password import change_password_route
from Routes.Logiin_Management.root_reset_pass import reset_password_route
from Routes.Role_Specific.app_link_upload import app_upload_link_route
from Routes.Role_Specific.link_dashboard import upload_link_dashboard_route
from addMissingfiels import missing_field_route
from Routes.Dashboard.date_wise_filter import date_filter_route
from Routes.Dashboard.task_overview import all_analatics_data_route
from Routes.Flag_Management.user_reject_route import user_reject_create_route
# from Routes.Pipelines.daily_report_gen import dailr_report_gen
from Routes.Pipelines.scrap_Json_upload import json_upload_routess

from Routes.Pipelines.report_gen_route import report_gen_route
import time
app = Flask(__name__)

CORS(app)

app.register_blueprint(login_route, url_prefix='/be/login')
app.register_blueprint(signup_route, url_prefix='/be/signup')
# app.register_blueprint(reset_password_link_gen_route, url_prefix='/be/resetPassLinkGen')
app.register_blueprint(change_password_route, url_prefix='/be/changePass')
app.register_blueprint(reset_password_route, url_prefix='/be/34dfgsvb')


app.register_blueprint(dashboard_data_route, url_prefix='/be/getAllData')     # Getting the dashboard of all 
app.register_blueprint(approved_dashboard_data_route, url_prefix='/be/getAllApprovedData') # Getting flaged data for all
app.register_blueprint(Reject_dashboard_data_route, url_prefix='/be/getAllRejectedData')
app.register_blueprint(analatics_data_route, url_prefix='/be/getAnalytics')
app.register_blueprint(date_filter_route, url_prefix='/be//searchByDate')
app.register_blueprint(all_analatics_data_route, url_prefix='/be/taskOverview')


app.register_blueprint(json_upload_route, url_prefix='/be/jsonUpload')
app.register_blueprint(pdf_download_route, url_prefix='/be/pdfDownload')
app.register_blueprint(pdf_download_all_route, url_prefix='/be/allPdfDownload')


app.register_blueprint(flag_create_route, url_prefix='/be/superiorAddFlag')
app.register_blueprint(user_flag_create_route, url_prefix='/be/userAddFlag')
app.register_blueprint(flag_reject_route, url_prefix='/be/superiorRejectFlag')
app.register_blueprint(user_reject_create_route, url_prefix='/be/userReject')
 
app.register_blueprint(data_search_route, url_prefix='/be/dataSearch')

app.register_blueprint(app_upload_link_route, url_prefix='/be/linkUpload')
app.register_blueprint(upload_link_dashboard_route, url_prefix='/be/appLinkAll')


app.register_blueprint(missing_field_route, url_prefix='/be/missfield')


app.register_blueprint(report_gen_route, url_prefix='/be/reportGen')

app.register_blueprint(json_upload_routess, url_prefix='/be/jsonUploadss')



# def scheduler_thread():
#     while True:
#         now = time.localtime()
#         if now.tm_hour == 16 :  # Specific time interval
#             dailr_report_gen()  # Execute the function
#             time.sleep(3600)  # Wait for 60 minutes
#         else:
#             time.sleep(60) 
# scheduler_thread = threading.Thread(target=scheduler_thread, daemon=True)
# scheduler_thread.start()

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
