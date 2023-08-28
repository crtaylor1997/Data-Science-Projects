#MSCI Derivative Spec Template

import numpy as np
import re
import pyodbc
import pandas as pd
import re

conn = pyodbc.connect(r'redacted)
cursor = conn.cursor()
#pd.set_option('display.max_columns',None)
x=pd.read_sql_query("SELECT * FROM tbvFIHoldingsEODCurrent WHERE AsOfDate="+date,conn)
m=pd.read_sql_query("SELECT Cusip,CurrencyName,CurrencyCode,	FirmCodeCategory FROM redacted",conn)
y = pd.read_sql_query("SELECT AsOfDate,CusipForShort,localnotional,notionalToUse,custodianaccountnumber	 FROM redacted WHERE AsOfDate="+nbd,conn)
k= pd.read_sql_query("SELECT * FROM tbvSecurityMaster",conn)

date = '2023-07-31'
nbd = '2023-07-31'


y['AsOfDate'] = date


m=m[['Cusip','CurrencyCode',	'FirmCodeCategory']]
y= y[['CusipForShort','localnotional','notionalToUse','custodianaccountnumber']]



MSCI=x[['AsOfDate','TypeOfSecurity','CompanyName','SecurityLongName','Cusip','CouponRate','PositionType','DateSecurityMatures','AccountNumber','IssueInformation']]
 


ffunds = ["FFUND" in str(n)for n in MSCI['IssueInformation']]
libor = ["LIBOR" in str(n) for n in MSCI['IssueInformation']]
sofr = ["SOFR" in str(n) for n in MSCI['IssueInformation']]

conditions = [ffunds,libor,sofr]
choices =[ "FFUND","LIBOR",'SOFR']

MSCI['ReferenceRate'] = np.select(conditions,choices,default = '')

#MSCI=MSCI.drop(columns=['IssueInformation'])

MSCI['Payment Type']= np.nan

MSCI=MSCI.merge(y,how='left',left_on=['Cusip','AccountNumber'],right_on=['CusipForShort','custodianaccountnumber']).drop_duplicates().drop(columns=['custodianaccountnumber'])

MSCI=MSCI.merge(m,how='left',on='Cusip').drop_duplicates()

MSCI['PrimaryAssetIDType']="LOCALID"


f=pd.read_excel("redacted\\MSCI Fields.xlsx")

f=f[f['DB Table Fields'].isna()]

MSCI.columns=['ASOfDate','Asset Class','AssetType','AssetName','Cusip','COUPON','LegType','MaturityDate','Fund','IssueInformation'	,'ReferenceRate',	'Payment Type','CusipForShort','Contract Size','Notional','Currency','Instrument Type','PrimaryAssetIDType']

for name in f['MSCI Template Fields'].tolist():
    MSCI[name]=''
MSCI

excluded =[#redacted]

MSCI=MSCI[[x not in excluded for x in MSCI['Fund']]]

#MSCI['Fund']=[n[:4] for n in MSCI['Fund']]

MSCI['OptionStyle']=0


InstruementType=[]

for x,y in list(zip(MSCI['Instrument Type'],MSCI['AssetType'])):
    if "SWAP"in y:
        InstruementType.append("SWAP")
    else:
       InstruementType.append(x)
MSCI['Instrument Type'] = InstruementType


MSCI=MSCI.drop_duplicates(subset=['Cusip','Fund','LegType'])

MSCI= MSCI[(MSCI['AssetType'] == 'TOTAL RETURN SWAP') | (MSCI['AssetType'] == 'SWAP') | (MSCI['AssetType'] == 'COMMODITY SWAP') | (MSCI['AssetType'] == 'SWAPS') ]

MSCI['Name'] = [x.replace('_','').replace('EL','').replace('FL','').replace('  ',' ') for x in MSCI['AssetName']]


names = MSCI['Name'].drop_duplicates().tolist()
infos = MSCI['IssueInformation'].drop_duplicates().dropna().tolist()
funds = MSCI['Fund'].drop_duplicates().tolist()




dfs = []
n_dfs=[]

#Fill in the blank data in the Long legs

for info in infos:
    for fund in funds:
        df = MSCI[(MSCI['IssueInformation']==info)&(MSCI['Fund']==fund)]
        df['COUPON'] = df['COUPON'].replace(0.00,np.nan)
        dfs.append(df)
       

        fillcols = ['MaturityDate','Contract Size',	'Notional',	'Currency',	'Instrument Type','CusipForShort','COUPON']
        
        fillobj = [df[x].drop_duplicates().dropna().tolist()[0] if df[x].drop_duplicates().dropna().tolist() != [] else ''  for x  in fillcols]
        
        #Break Each Swap into seperate dfs and fill the nans with the correct values in 'fillobj'
        for col,obj in list(zip(fillcols,fillobj)):
            df[col]=df[col].fillna(obj)
        dfs.append(df)
    for df in dfs:
            drop_index=[]
            if df.shape[0] == 1:
                n_dfs.append(df)
            else:
                
                #For each df chunk drop rows that are not the Equity Leg.  
                for x,y,z in list(zip(df['LegType'].tolist(),df['IssueInformation'].astype(str).tolist(),df.index.tolist())):

                    #Regex to match rows that contain a P or an R followed by some characters to Pay and Receive matches
                    pay_match = re.search("(?<![a-z])p[ ]?[a]?[y]?", y.lower())
                    receive_match = re.search("(?<![a-z])r[ ]?[e]?[c]?[e]?", y.lower())
                    
                    # Combine this with the Leg Type to determine which rows are NOT equity legs and add the indices to a list to drop them from the output
                    if pay_match and x == 'S':
                        drop_index.append(z)
                    elif receive_match and x == 'L':
                        drop_index.append(z)
                n_dfs.append(df.drop(drop_index))



MSCI=pd.concat(n_dfs)
MSCI = MSCI.merge(k[['Cusip','DatedDate','NumberOfPaymentsPerYear']],how='left', left_on = ['CusipForShort'],right_on=['Cusip'])

MSCI=MSCI.rename(columns = {'DatedDate':'StartDate','NumberOfPaymentsPerYear':'PaymentFrequency'})

RM = pd.read_excel("redacted//Common BBG tickers mapped to RiskManager IDs.xlsx")


conditions=[[x in y for y in MSCI['AssetName']] for x in RM['BBG Ticker']]
choices = RM['RM Identifier'].tolist()

MSCI['RM Ticker'] = np.select(conditions,choices,default='')


MSCI['RM Ticker'] = np.select(conditions,choices,default='')

choices = RM['BBG Ticker'].tolist()
MSCI['BBG Ticker'] = np.select(conditions,choices,default='')

MSCI=MSCI.drop(columns=['IssueInformation','Name','Cusip_y'])

MSCI = MSCI.rename(columns={'Cusip_x':'PrimaryAssetID','Contract Size':'ContractSize','Instrument Type':'InstrumentType','RM Ticker':'Underlier ID'})

MSCI['InstrumentType'] = MSCI['InstrumentType'].str.replace('SWAP','SWAPS')

MSCI['AssetType'] = MSCI['AssetType'].str.replace('TOTAL RETURN SWAP','TOTAL RETURN SWAPS')

MSCI['UnderlierIDType'] = 'RM Ticker'

MSCI['PaymentFrequency']=MSCI['PaymentFrequency'].astype(str)

MSCI['PaymentFrequency'] = MSCI['PaymentFrequency'].str.replace('4.0','3m').str.replace('12.0','1m').str.replace('0.0','').str.replace('nan','')

MSCI=MSCI.drop_duplicates()

MSCI['Account Number']=MSCI['Fund']
MSCI['Fund']=[n[:4] for n in MSCI['Fund']]

col_order = ['ASOfDate', 'Asset Class', 'AssetType', 'AssetName', 'PrimaryAssetID', 'ContractSize',
       'COUPON', 'Currency' , 'DealSpread','InstrumentType','LegType', 'MaturityDate', 'Notional', 'OptionStyle',
       'OptionType','PaymentFrequency', 'ReferenceRate', 'StartDate','StrikePrice','BBG Ticker','Underlier ID', 'UnderlierIDType', 
       'Payment Type',
        'PrimaryAssetIDType', 'StartDate',  'AccrualBasis', 'AccrualType',
       'AmountIssued', 'AssetPrice', 'AssetPriceCurrency', 'Cap', 'Country',
       'CouponFrequency', 'FirstCouponDate', 'FixedRecovery', 'Floor',
       'IVExposure', 'LastCouponDate', 'Margin', 'Multiplier', 'PeriodicCap',
       'PeriodicFloor', 'QuoteCurrency', 'RateTerm', 'RecoveryRate',
       'ResetFrequency', 'UnderlierPrice','Fund','Account Number' 
       ]

MSCI = MSCI[col_order]

MSCI['Asset Class'] = 'TOTAL RETURN SWAP EQUITY'

startdate=MSCI['StartDate'].iloc[:,3].tolist()
MSCI['StartDate']=startdate
MSCI=MSCI.loc[:,~MSCI.columns.duplicated()].copy()

MSCI['AssetName']=MSCI['AssetName'].str.replace(redacted)

MSCI['COUPON'] = MSCI['COUPON'].replace('',0.00)
MSCI['Payment Type']=["Floating" if (str(x)=='nan') or (x==0.0) else "Fixed" for x in MSCI['COUPON']]
MSCI['Underlier ID'] = MSCI['Underlier ID'].str.replace(redacted).str.replace(redacted)


MSCI.to_csv("redacted\\MSCI Derivatives Template_07312023.csv",index=False)
 
 
