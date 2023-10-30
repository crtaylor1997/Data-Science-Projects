from msci.bdt.context.ServiceClient import ServiceClient
from msci.bdt.context.BPMClient import BPMClient
import pandas as pd
import base64
import zipfile
import time
import os

"""
Wrapper Functions to interact with the MSCI SOAP API. The first function places an order for a specifc HVR at a given date.
The second function downloads the data as an encoded string. It decodes and places the files in a directory. The final functions
are os based to prepare the files for processing.
"""

def SubmitHvrReport(url,user_id,password,client_id,Hvr_name,Hvr_owner,date):

    bpm_client = BPMClient(url,user_id,password,client_id)
    submit_hvr = bpm_client.factory.create('SubmitHVRJobRequest')
    hvr_report_request_params = bpm_client.factory.create('HvrReportRequestParams')
    hvr_definition = bpm_client.factory.create('HvrDefinition')
    hvr_definition._Name = Hvr_name
    hvr_definition._Owner = Hvr_owner
    hvr_report_request_params.HvrDefinition = hvr_definition
    hvr_report_request_params.CycleDate = date
    submit_hvr.HvrReportRequestParams = hvr_report_request_params
    submit_hvr.User = user_id
    submit_hvr.Client = client_id
    submit_hvr.Password = password
    response = bpm_client.service.SubmitHVRJob(hvr_report_request_params)
    return(response)



def DownloadHvrReports(url,user_id,password,client_id,Hvr_name,Hvr_owner,date,destination_path):
    
    bpm_client = BPMClient(url,user_id,password,client_id)
    hvr_report_request_params = bpm_client.factory.create('HvrReportRequestParams')
    hvr_definition = bpm_client.factory.create('HvrDefinition')
    hvr_reports_request = bpm_client.factory.create('GetHvrReportsRequest')
    hvr_report_request_params.HvrDefinition = hvr_definition
    hvr_report_request_params.CycleDate = date
    hvr_reports_request.HvrReportRequestParams = hvr_report_request_params
    hvr_definition._Name = Hvr_name
    hvr_definition._Owner = Hvr_owner
    hvr_report_request_params.HvrDefinition = hvr_definition
    hvr_reports_request.User = user_id
    hvr_reports_request.Client = client_id
    hvr_reports_request.Password = password
    hvr=bpm_client.service.GetHvrReports(hvr_report_request_params)


    coded_string = str(hvr.BinaryData)
    decoded_string = base64.b64decode(coded_string)

    with open(destination_path + '\\' + hvr.FileName,'wb') as fout:
        fout.write(decoded_string)
    print(hvr.FileName + " Downloaded to Path")


def Unzipogs(PATHS,PATH,report,early_date):
    zip_path = PATHS[PATH]+'\\'+early_date+'_'+report+'.zip'
    with open(zip_path) as f:
        archive = zipfile.ZipFile(zip_path)
        archive.extractall(PATHS[PATH])
    PATH = PATHS[PATH]
    cib_files=os.listdir(PATH)
    log_files = [x for x in cib_files if 'RejectedAssets' in x or 'JobSummary' in x]
    for log in log_files:
        os.remove(PATH+'\\'+log)
    time.sleep(5)
    

def ArchiveZip(PATH,early_date,report,PATHS):
    os.rename(PATH+'\\'+early_date+'_'+report+'.zip',PATHS['ARCHIVE_PATH']+'\\'+early_date+'_'+report+'.zip')