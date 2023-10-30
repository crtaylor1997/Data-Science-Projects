
import pandas as pd
from MSCI_Reports import run_report,read_msci_outputs
from Manager_Drilldown_Functions import CheckTimeSeriesStatus
from Manager_Drilldown_Functions import FlattenBarraVol,FlattenOne
from Manager_Drilldown_Functions import CheckBarraVolStatus
from pandas.tseries.offsets import *
from datetime import  datetime, timedelta
from dateutil.relativedelta import relativedelta
from multiprocessing import Pool
import numpy as np
import os

"""
These are auxillary functions that support the fundemntal data cleaning and calculations
functions defined in Manager_Drilldown_Functions.py. These functions are either aggegates
meant to simplify and build on functions already written, or custodial functions that delete
unnessesary or inorrect data, archive data etc....
"""

def run_outstanding_reports(dates_to_run,suffix):

    """
    Takes the output from the CheckTimeSeriesStatus() or CheckBarraVolStatus() functions.
    runs reports that need to be updated via the MSCI API. Relies on the run_report() from
    MSCI_Repots.py. Make sure to clear the CIB_PATH by running the AchiveReports() function
    contained in this file before running this function. Suffix can either be '_timeseries' or
    '_barra' depending on which type of report is being run.
    """

    funds_to_run = dates_to_run['Short Code'].tolist()
    for fund in funds_to_run:
        dates=dates_to_run[dates_to_run['Short Code']==fund]['Cib Format Date'].tolist()
        args = [(fund+suffix,date) for date in dates[0]]
        with Pool(len(dates)) as p:
            p.starmap(run_report,args)
            

def DeleteLogs(PATHS):

    #Deletes unnessesary Log files that are downloaded by the API

    CIB_PATH = PATHS['CIB_PATH']
    cib_files=os.listdir(CIB_PATH)
    log_files = [x for x in cib_files if 'Log' in x]
    for log in log_files:
        os.remove(CIB_PATH+log)


def AchiveReports(PATHS, ARCHIVE_PATH):

    #Send reports to archive, choose correct path based on which report type you are running

    CIB_PATH = PATHS['CIB_PATH']

    cib_files=os.listdir(CIB_PATH)
    for cib in cib_files:
        os.rename(CIB_PATH+cib,ARCHIVE_PATH+cib)       

        
def FlattenAllTimeseries(PATHS,CIB_PATH,ts,name_key):

    """
    Wrapper function that tranforms all reports in a directory from XML to tabular format. 
    Has built in logic for x exposure replacements. Relies on the FlattenOne() funtions
    and the read_msci_reports() function.
    """

    
    
    CIB_PATH = PATHS['CIB_PATH']
    name_key = {y:x for x,y in list(zip(name_key['Fund Name'],name_key['RM ShortCode']))}
    dfs=[]
    for f in os.listdir(CIB_PATH):
        sfts = pd.read_excel(CIB_PATH+f)
        sfts=read_msci_outputs(sfts)
        if sfts[1]['Portfolio Name'] == 'xMasterExp':
            pass
        elif sfts[1]['Portfolio Name'] == 'xMaster':
             x_dfs = LocatexReports(PATHS)
             x=AdjustMExposure(x_dfs['xMaster'],x_dfs['xMasterExp'],PATHS)
             dfs.append(x)
             
        else:
            sfts = FlattenOne(sfts)
            dfs.append(sfts)
    sfts_cd=pd.concat(dfs)
    sfts_cd = sfts_cd.iloc[:,:-1]
    sfts_cd.columns = ts.columns
    sfts_cd=sfts_cd.replace({'Fund':name_key})
    return(sfts_cd)


"""
Wrapper functions to clean up and run reports.
"""

def RunTimeSeries(PATHS,ts,name_key,hedge_status):


    dates_to_run=CheckTimeSeriesStatus(ts,name_key,hedge_status)
    run_outstanding_reports(dates_to_run,'_timeseries')
    DeleteLogs(PATHS['CIB_PATH'])

def FormatTimeSeries(name_key,PATHS):

    new_ts=FlattenAllTimeseries(PATHS['CIB_PATH'],ts,name_key)
    ts_1 = pd.concat([ts,new_ts]).drop_duplicates(subset=['Date', 'Fund', 'Security Category', 'Region', 'Sector', 'Market Value'],keep='last')
    ts_1.to_csv(PATHS['TIME_SERIES_PATH'],index=False,sheet_name='Aggregated Fund Level Data')
    AchiveReports(PATHS['CIB_PATH'],PATHS['TIMESERIES_ARCHIVE_PATH'])
    ts=ts_1

def UpdateBarraVol(PATHS,flattened_barra,name_key,hedge_status):

    barra_dates_to_run = CheckBarraVolStatus(flattened_barra,name_key,hedge_status)
    print(barra_dates_to_run)
    run_outstanding_reports(barra_dates_to_run,'_barra')
    DeleteLogs(PATHS)
    new_barra=pd.concat([flattened_barra,FlattenBarraVol(PATHS['CIB_PATH'],name_key)]).drop_duplicates(keep='last')
    new_barra.to_csv(PATHS['AGGREGATED_BARRA_PATH'])
    AchiveReports(PATHS,PATHS['BARRA_ARCHIVE_PATH'])


def LocateMReports(PATHS):
    """
    This function locates specical x reports in the CIB_PATH so it can initiate the exposure replacement protocol.
    """
    CIB_PATH = PATHS['CIB_PATH']
    x_dfs = {}
    for f in os.listdir(CIB_PATH):
        if 'Log' not in f:
            df=pd.read_excel(PATHS['CIB_PATH']+f)
            portfolio=read_msci_outputs(df)[1]['Portfolio Name']
            print(portfolio)
            if portfolio == 'redacted':
                x_dfs['redacted'] = df
            elif portfolio == 'redacted':
                x_dfs['redacted'] = df
    if len(x_dfs) != 2:
        print('Correct Exposures not Present, check CIB PATH')
    else:
        return(x_dfs)


def AdjustExposure(x,exp,PATHS):

    """
    MSCI cannot model redacted exposures so we must replace them with the correct figures.
    """

    exp=read_msci_outputs(exp)[0].reset_index(drop=True)
    x = FlattenOne(read_msci_outputs(x))
    exp = exp.iloc[:1,1:]

    #Load in the sample report for the correct columns. x's report is missing several columns so concatenating it with the correct columns will normalize it.

    sample_ts = pd.read_excel(PATHS['SAMPLE_TS_PATH'])
    sample_ts = FlattenOne(read_msci_outputs(sample_ts)).iloc[:0,:]
    x=pd.concat([sample_ts,x])
    x.columns.tolist()
    x_lst = x.loc[0].tolist()
    exp_lst = exp.loc[0].tolist()[3:]
    x_adj = x_lst[:7] + x_exp_lst + x_lst[11:]
    x_corrected = pd.DataFrame([x_adj],columns=x.columns)
    return(x_corrected)



def RemoveBadData(dates,funds,ts):

    y=ts[~(ts['Fund'].isin(funds) & ts['Date'].isin(dates))]
    x=ts[(ts['Fund'].isin(funds) & ts['Date'].isin(dates))]

    if y.shape[0] + x.shape[0] == ts.shape[0]:
        return(y)