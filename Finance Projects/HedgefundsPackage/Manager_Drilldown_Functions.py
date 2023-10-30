import os
import pandas as pd
import numpy as np
from pandas.tseries.offsets import *
from datetime import  datetime, timedelta
from dateutil.relativedelta import relativedelta

"""
These function contains the bulk of the code that runs the data cleaning
and calculations to build the datasets that flow into the main Hedge Funds 
Power BI Visuals. Also contains funtions to calculate which funds need 
to be fed into the API.
"""


def FlattenBarraVol(BARRA_PATH,name_key):

    #Flattens the BarraVol reports and calculates rolling averages

    name_key = {y:x for x,y in list(zip(name_key['Fund Name'],name_key['RM ShortCode']))}
    files = os.listdir(BARRA_PATH)
    fs=[]
    dfts=[]
    for filename in files:  
        factors_dict=pd.read_excel(BARRA_PATH+filename,sheet_name=None,)
        factors_dict = {x:factors_dict[x] for x in factors_dict.keys() if 'Exposure Risk' in x and 'Summary' not in x}
        dfs=[]
        
        for factors in factors_dict.values():
            fund_name = factors[factors.columns[2]].loc[8]
            date = factors.loc[9].tolist()[2]
            
            fact=factors.dropna(subset = [factors.columns[-1]]).reset_index(drop=True).iloc[:,3:]

            cols =fact.loc[0]
            ls=fact.loc[1]
            columns = [x + ' ' + y for x,y in list(zip(cols,ls))]
            columns.insert(0,'Factor Name')
            if fund_name == 'redacted' or 'redacted':
                factors=factors.iloc[18:,2:]
            else:
                factors=factors.iloc[19:,2:]
            factors.columns=columns
            factors['Fund Name'] = fund_name
            factors['Date']= date
            
            dfs.append(factors)
            x=pd.concat(dfs)
        fs.append(x)
    y=pd.concat(fs)


    y=y.rename(columns={'Fund Name':'Short Code'})
    y['Fund Name'] = y['Short Code'].map(name_key)
    y=y[y['Short Code']!=y['Fund Name']]
    y['Date'] = pd.to_datetime(y['Date'])
    y['Date'] = [str(x).split('-')[0] + '-'+str(x).split('-')[1] for x in y['Date']]
    y=y.drop_duplicates(subset=['Factor Name','Date','Short Code'],keep='last')

    dfs=[]

    for mgr in y['Short Code'].drop_duplicates().tolist():
        mgr_chunk=y[y['Short Code']==mgr]

        for date in mgr_chunk['Date'].drop_duplicates().tolist():

            factors_list = mgr_chunk['Factor Name'].drop_duplicates().tolist()
            date_chunk=mgr_chunk[mgr_chunk['Date']==date].reset_index(drop=True)
            cur_fact = date_chunk['Factor Name'].drop_duplicates().tolist()
            s = set(cur_fact)
            factors_add = [x for x in factors_list  if x not in s]
            
            num_cols = ['Factor Loading Total',	'IVaR Hist 95 Total',	'IVaR Hist 95 long','IVaR Hist 95 short','Factor Loading long',	'Factor Loading short']
            str_cols = ['Fund Name','Short Code','Date']
            factor_name = ['Factor Name']

            num_town = [[0 for x in num_cols] for y in factors_add]
            str_town = [[date_chunk[x].loc[0] for x in str_cols ]for z in factors_add]
            str_town
            tot_cols=num_cols+str_cols+factor_name
            big_town = [x+y+[z] for x,y,z in list(zip(num_town,str_town,factors_add))]
            df_town=pd.DataFrame(big_town,columns=tot_cols)
            df=pd.concat([date_chunk,df_town])
            dfs.append(df)
            

    y=pd.concat(dfs)

    Risk =['GEMLT Dividend Yield',
    'GEMLT Earnings Quality',
    'GEMLT Earnings Variability',
    'GEMLT Earnings Yield','GEMLT Beta','GEMLT Book-to-Price','GEMLT Investment Quality','GEMLT Momentum','GEMLT Profitability','GEMLT Growth','GEMLT Long-Term Reversal','GEMLT Dividend Yield','GEMLT Size','GEMLT Mid Capitalization','GEMLT Leverage','GEMLT Liquidity','GEMLT Residual Volatility']

    world = ['GEMLT World','*Granular']
    Country = [x for x in y['Factor Name'].drop_duplicates() if 'Mkt' in x]
    conditions = [[x in Risk for x in y['Factor Name']],[x in Country for x in y['Factor Name']],[x =='Total' for x in y['Factor Name']],[x in world for x in y['Factor Name']],[x =='*Non-Equity Risk' for x in y['Factor Name']],[x =='*Residuals' for x in y['Factor Name']]]
    choices = ['Risk','Country','Total','World','*Non-Equity Risk','*Residuals']

    y['GEMLT Category'] = np.select(conditions,choices,default='Industry')

    totals=y[y['Factor Name']=='Total'][['Short Code',	'Date','IVaR Hist 95 Total'	]]
    totals['IVar Total Rolling 3M AVG'] = totals.rolling(3,min_periods=1).mean()['IVaR Hist 95 Total']
    totals['IVar Total Rolling 3M AVG'] = totals['IVaR Hist 95 Total']
    factors_vol=totals[['Short Code',	'Date','IVar Total Rolling 3M AVG']].merge(y,how='right',on=['Short Code','Date'])



   
    factors_vol=factors_vol.sort_values(['Date','Short Code'])
    factors_vol=factors_vol[factors_vol['IVaR Hist 95 Total']!='Total']
    factors_vol['IVaR Hist 95 Total (% of Total)'] = (factors_vol['IVaR Hist 95 Total']/factors_vol['IVar Total Rolling 3M AVG'])
    factors_vol['Factor Loading 6 Mo Rolling Avg'] = factors_vol.groupby(['Short Code',	'Date','Factor Name'])['Factor Loading Total'].transform(lambda x: x.rolling(6,1).mean())
    factors_vol['IVaR Hist 95 Total (% of 3M AVG)'] = factors_vol.groupby(['Short Code','Factor Name'])['IVaR Hist 95 Total (% of Total)'].transform(lambda x: x.rolling(3,1).mean())

    factors_vol=factors_vol.drop_duplicates()
    return(factors_vol)


def CheckBarraVolStatus(factors_vol, name_key,hedge_status):
    name_key = {y:x for x,y in list(zip(name_key['Fund Name'],name_key['RM ShortCode']))}
    excluded_funds=['redacted']
    excluded_funds_barra = excluded_funds + ['redacted']
    hedge_status=hedge_status[hedge_status['Status']=='RELEASED']
    hedge_status=pd.DataFrame(hedge_status.groupby(['Fund Short Name'],sort=False)['Starting Date'].max()).reset_index()
    latest_dates_barra=factors_vol[['Short Code','Date']].drop_duplicates()
    latest_dates_barra = pd.DataFrame(latest_dates_barra.groupby(['Short Code'],sort=False)['Date'].max()).reset_index()

    missing_dates=latest_dates_barra.merge(hedge_status,how='left',left_on=['Short Code'],right_on=['Fund Short Name'])
    missing_dates['Date']=[MonthEnd().rollforward(x) for x in missing_dates['Date']]
    missing_dates=missing_dates[missing_dates['Date']<missing_dates['Starting Date']]
    missing_dates['Month delta'] = [x.month-y.month if x.year==y.year else x.month+12-y.month for x,y in list(zip(missing_dates['Starting Date'],missing_dates['Date']))]
    dates=[]
    for date,delta in list(zip(missing_dates['Starting Date'],missing_dates['Month delta'])):
        new_dates=[]
        for n in range(delta):
            new_dates.append(date-relativedelta(months=n))
        dates.append(new_dates)
    missing_dates['Dates_to_run'] = dates
    missing_dates = missing_dates[missing_dates['Date']>='2022-10-31']
    missing_dates['Cib Format Date']=[[str(x).replace(' 00:00:00','').replace('-','') for x in n ] for n in missing_dates['Dates_to_run'] ]
    missing_dates= missing_dates[~missing_dates['Short Code'].isin(excluded_funds_barra)]
    return(missing_dates)

def CheckTimeSeriesStatus(ts,name_key,hedge_status):
    name_key = {y:x for x,y in list(zip(name_key['Fund Name'],name_key['RM ShortCode']))}
    excluded_funds=['redacted']
    ts=ts.dropna(subset='Fund')
    ts['Fund']=[x.strip() for x in ts['Fund']]
    hedge_status=hedge_status[hedge_status['Status']=='RELEASED']
    hedge_status=pd.DataFrame(hedge_status.groupby(['Fund Short Name'],sort=False)['Starting Date'].max()).reset_index()
    ts['Date']=pd.to_datetime(ts['Date'])
    name_key = {y.strip():x.strip() for x,y in name_key.items()}
    ts['Short Code'] = ts['Fund'].map(name_key)

    latest_dates_ts=ts[['Short Code','Date','Fund']].drop_duplicates()
    latest_dates_ts = pd.DataFrame(latest_dates_ts.groupby(['Short Code','Fund'],sort=False)['Date'].max()).reset_index()

    missing_dates=latest_dates_ts.merge(hedge_status,how='left',left_on=['Short Code'],right_on=['Fund Short Name'])
    missing_dates['Date']=[MonthEnd().rollforward(x) for x in missing_dates['Date']]
    missing_dates=missing_dates[missing_dates['Date']<missing_dates['Starting Date']]
    missing_dates['Month delta'] = [x.month-y.month if x.year==y.year else x.month+12-y.month for x,y in list(zip(missing_dates['Starting Date'],missing_dates['Date']))]
    dates=[]
    for date,delta in list(zip(missing_dates['Starting Date'],missing_dates['Month delta'])):
        new_dates=[]
        for n in range(delta):
            new_dates.append(date-relativedelta(months=n))
        dates.append(new_dates)
    missing_dates['Dates_to_run'] = dates

    missing_dates = missing_dates[missing_dates['Date']>='2022-10-31']
    missing_dates['Cib Format Date']=[[str(x).replace(' 00:00:00','').replace('-','') for x in n ] for n in missing_dates['Dates_to_run'] ]
    missing_dates= missing_dates[~missing_dates['Short Code'].isin(excluded_funds)]
    return(missing_dates)

def FlattenOne(msci_output):
    """
    This function takes in an XML structured output file from MSCI and flattens it into a to a tabular format
    """
    ts_f=msci_output[0]
    ts_f = ts_f.reset_index(drop=True).iloc[2:,:].reset_index(drop=True)
    ts_levels = ts_f[['Level','Name']]
    ts_levels
    levels_lst=[]
    
    for l,n in list(zip(ts_levels['Level'],ts_levels['Name'])):
        if l == 1:
            levels_tup = ['','']
            category = n   
        elif l == 2: 
            region = n
        elif l == 3:
            levels_tup = [category,region]
            levels_lst.append(levels_tup)
          
    sector = [x[0] for x in levels_lst]
    region = [x[1] for x in levels_lst]
    ts_three = ts_f[ts_f['Level']==3]
    cols = ts_three.columns.tolist()[2:]
    ts_three['Security Category'] = sector
    ts_three['Region'] = region
    fund = msci_output[1]['Portfolio Name']
    date = msci_output[1]['As-Of Date']
    ts_three['Fund'] = fund
    ts_three['Date'] = date
    ts_three=ts_three.rename(columns={'Name':'Sector'})
    ts_three = ts_three[['Date','Fund','Security Category','Region','Sector']+cols]
    ts_three=ts_three.reset_index(drop=True)
    return(ts_three)