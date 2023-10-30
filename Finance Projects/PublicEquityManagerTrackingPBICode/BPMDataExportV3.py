#Exposure Bulk Data Export
import pandas as pd
import re
import numpy as np
import os
from pandas.tseries.offsets import MonthEnd

"""
These functions create the datsets that feed into the PubEQ Manager Tracking Power BI dashboard. 
the data is fetched  via SOAP API, tranformed, and then saved into Parquet and CSV files. 
"""


def TransformPositionData(POS_PATH,data,early_date):

    """
This function takes in any position level BPM HVR and transforms it into a flat file that can be better used for analysis.
 It also performs several bucketing and mapping operations as well as data type transferring.
   It takes in the location of the Dataset and the starting date (obtained in API calls) and returns a flattened dataframe.
    """
    
    #Read in Data and remove blank rows
    Bpm = pd.read_csv(POS_PATH+'\\'+data,header=12).dropna(axis=0,how='all')
    dates=Bpm[Bpm['Asset ID']=='Analysis Date:']['GICS Sector'].tolist()
    dates.insert(0,early_date)
    Bpm.index
    date_column = []
    n=0
    #normalizes the index
    for i in range (len(Bpm.index.tolist())):
        date_column.append(dates[n])
            
        if Bpm['Asset ID'].loc[i]=='Analysis Date:':

            n=n+1
    #Convert Columns to the correct data types            
    Bpm['Date']=date_column
    Bpm=Bpm[Bpm['Asset Name'].notna()]
    Bpm=Bpm[Bpm['Portfolio (%)']!='Portfolio (%)']
    num_columns = ['Portfolio (%)',
    
        'Benchmark (%)',
        'Active Weight (%)',
        'Eff Weight (%)',
        'Eff Bmk Weight (%)',
        'Eff Active Weight (%)','Dividend Yield (%)'
        ]
    

    for n in num_columns:
        Bpm[n]= [re.sub("[^0-9.-]","",x) if str(x) != 'nan' else x for x in Bpm[n].tolist()]
            
        Bpm[n]=Bpm[n].astype('float64')

    #creates market cap categories

    Bpm['Market Capitalization']=Bpm['Market Capitalization'].astype('float64')
    Bpm['Mkt Value']=Bpm['Mkt Value'].astype('float64')
    conditions=[Bpm['Market Capitalization']>20000000000,((Bpm['Market Capitalization']<=20000000000) & (Bpm['Market Capitalization']>5000000000)),((Bpm['Market Capitalization']<=5000000000) & (Bpm['Market Capitalization']>1000000000)),Bpm['Market Capitalization']<=1000000000]

    choices = ['Large > $20B','Mid $5-20B','Small $1-5B','Micro <1B']
    Bpm['Market Cap Category']=np.select(conditions,choices,default = 'No Information')
    Bpm=Bpm.rename(columns={'Benchmark (%)':'Benchmark (%) MSCI World'})
    Bpm_header = pd.read_csv(POS_PATH+'//'+data,nrows=11).dropna(axis=0,how='all')

    #Read in country/ region mapping file and use it to create region tags
    Bpm['Portfolio'] = Bpm_header['redacted'].loc[1]

    codes=pd.read_csv("redacted")
    codes=codes[['Country','Alpha-3 code']]
    codes['Alpha-3 code']=codes['Alpha-3 code'].str.replace('"',"").str.strip()
    regions = pd.read_excel("redacted")
    regions['Country']=regions['Country'].str.lower()
    codes['Country']=codes['Country'].str.lower()
    regions=regions.merge(codes,how='left',on='Country')
    regions=regions.dropna(subset=['Alpha-3 code'])
    regions['MSCI Region'].drop_duplicates().tolist()

    EM_Americas = regions[regions['MSCI Region'] == 'EM Americas']['Alpha-3 code'].tolist()
    DM_Pacific = regions[regions['MSCI Region'] ==  'DM Pacific']['Alpha-3 code'].tolist()
    DM_Europe_ME = regions[regions['MSCI Region'] ==  'DM Europe & ME']['Alpha-3 code'].tolist()
    EM_EMEA = regions[regions['MSCI Region'] ==  'EM EMEA']['Alpha-3 code'].tolist()
    EM_Asia = regions[regions['MSCI Region'] ==  'EM Asia']['Alpha-3 code'].tolist()
    DM_Americas = regions[regions['MSCI Region'] ==  'DM Americas']['Alpha-3 code'].tolist()

    conditions = [[x in EM_Americas for x in Bpm['Country']],[x in DM_Pacific for x in Bpm['Country']],[x in DM_Europe_ME for x in Bpm['Country']],[x in EM_EMEA  for x in Bpm['Country']], [x in EM_Asia  for x in Bpm['Country']],[x in DM_Americas for x in Bpm['Country']]]
    choices = regions['MSCI Region'].drop_duplicates().tolist()[1:]

    Bpm['Region'] = np.select(conditions,choices,'Other')
    #Roll forward dates to the last day of the month
    Bpm['Date'] = pd.to_datetime(Bpm['Date'])+ MonthEnd(0)
    return(Bpm)

#Portfolio_Exp=pd.concat([TransformPositionData(PATH,x,early_date) for x in os.listdir(PATH)])

def MapBenchmarks(BMK_MAP_PATH,MGR_CODE_PATH,BMK_NAME_PATH):
    benchmark_mapping=pd.read_excel(BMK_MAP_PATH)
    benchmark_mapping=benchmark_mapping.iloc[:,1:]
    mgr_code = pd.read_excel(MGR_CODE_PATH)
    mgr_code=mgr_code.iloc[1:,3:]
    mgr_code.columns= ['b_Code','b_Name']
    benchmark_mapping=benchmark_mapping.merge(mgr_code,how='left',on='b_Name')
    bmk_name_map = pd.read_excel(BMK_NAME_PATH)
    bmk_name_map ={x:y for x,y in list(zip(bmk_name_map['Name'].tolist(),bmk_name_map['b_Name'].tolist()))}
    benchmark_mapping=benchmark_mapping.replace(bmk_name_map).dropna(subset='Region BM')

    benchmark_mapping['b_Code'] = benchmark_mapping['b_Code'].str.replace('COMP_','')
    benchmark_mapping = benchmark_mapping.replace('MSCI USA MICRO CAP','MSCI USA MICRO CAP - Daily')
    benchmark_mapping = benchmark_mapping.replace('CHINA A - Daily','MSCI CHINA A - Daily')
    benchmark_mapping = benchmark_mapping.replace('USA VALUE - Daily','MSCI USA VALUE - Daily')
    return benchmark_mapping

def RenameBMKS(BMK_PATH):
    for filename in os.listdir(BMK_PATH):

        if '_' in filename:
            source = BMK_PATH + '\\' + filename
            #dest = filename.split('_')[1]
            dest = BMK_PATH +'\\' + filename.split('_')[1] + '.csv'
            os.rename(source,dest)

def CalcBenchmarks(BMK_PATH,early_date):


    #Calculate Custom Benchmarks

    ######## ValueAct custom BMK###################################################################################################################################################

    MSCI_US=TransformPositionData(BMK_PATH,'MSCI USA - Daily.csv',early_date)
    MSCI_US_MID_VALUE = TransformPositionData(BMK_PATH,'MSCI USA MID VALUE - Daily.csv',early_date)
    MSCI_US_MID_VALUE['Portfolio (%)'] = MSCI_US_MID_VALUE['Portfolio (%)']*0.5
    MSCI_US['Portfolio (%)'] = MSCI_US['Portfolio (%)']*0.5
    ValueAct_custom_BMK=pd.concat([MSCI_US_MID_VALUE,MSCI_US])


    ######## Emminence custom BMK ###################################################################################################################################################

    MSCI_US_MIDCAP = TransformPositionData(BMK_PATH,'MSCI USA MID CAP - Daily.csv',early_date)
    MSCI_US=TransformPositionData(BMK_PATH,'MSCI USA - Daily.csv',early_date)
    MSCI_US['Portfolio (%)'] = MSCI_US['Portfolio (%)']*0.3
    MSCI_US_MIDCAP['Portfolio (%)'] = MSCI_US_MIDCAP['Portfolio (%)']*0.7
    Emminence_custom_BMK = pd.concat([MSCI_US,MSCI_US_MIDCAP])

    ################# Janchor custom BMK ################################################################################################################################################

    MSCI_AP = TransformPositionData(BMK_PATH,'MSCI AC ASIA PACIFIC - Daily.csv',early_date)
    MSCI_ASIA_EX_JAPAN = TransformPositionData(BMK_PATH,'MSCI AC ASIA ex JAPAN - Daily.csv',early_date)
    MSCI_AP['Portfolio (%)'] = MSCI_AP['Portfolio (%)']*0.5
    MSCI_ASIA_EX_JAPAN['Portfolio (%)'] = MSCI_ASIA_EX_JAPAN['Portfolio (%)']*0.5

    janchor_custom_bmk = pd.concat([MSCI_AP,MSCI_ASIA_EX_JAPAN])

    ##################################### Petra Custom BMK ################################################################################################################################
    MSCI_KOREA = TransformPositionData(BMK_PATH,'MSCI KOREA - Daily.csv',early_date)
    if early_date < '2023-06-01':
        
        MSCI_KOREA_SMALLCAP = TransformPositionData(BMK_PATH,'MSCI KOREA SMALL CAP - Daily.csv',early_date)

        MSCI_KOREA_SMALLCAP = MSCI_KOREA_SMALLCAP[MSCI_KOREA_SMALLCAP['Date']<'2023-06-01']
        MSCI_KOREA = MSCI_KOREA[MSCI_KOREA['Date']>'2023-06-01']

        petra_custom_bmk = pd.concat([MSCI_KOREA_SMALLCAP,MSCI_KOREA])
    
    else:
        petra_custom_bmk = MSCI_KOREA

    #Create Dict of b_Name of benchmarks to Transformed dataframes
    Benchmarks = {filename.split('.')[0].strip():TransformPositionData(BMK_PATH,filename,early_date) for filename in os.listdir(BMK_PATH)}
    Benchmarks['ValueAct Custom Benchmark'] = ValueAct_custom_BMK
    Benchmarks['Emminence Custom Benchmark'] = Emminence_custom_BMK 
    Benchmarks['Janchor Opp Custom BM'] = janchor_custom_bmk
    Benchmarks['Petra Custom Benchmark'] = petra_custom_bmk
    
    return Benchmarks



def CombinePositionData(benchmark_mapping,Benchmarks,Portfolio_Exp):

    
    """
    Maps each portfolio to its assigned benchmark and calculates asset level active weights for all postions
    """
    portfolios=[]
    excluded_portfolios = ['redacted']
    
    Portfolio_Exp=Portfolio_Exp[~Portfolio_Exp['Portfolio'].isin(excluded_portfolios)]
    subset_cols =['Portfolio (%)','Asset Name','Asset ID','Date','Region','Country','Market Cap Category','GICS Sector','Inst. Sub-type','Price','BARRAID']
    #Loop Through each portfolio in the dataset, grab its assigned benchmarks from the dictionary thar was built in the previous step and joins the two.
    for portfolio in Portfolio_Exp['Portfolio'].drop_duplicates().tolist():

        test_exp = Portfolio_Exp[Portfolio_Exp['Portfolio']==portfolio]
        test_benchmark_mapping=benchmark_mapping[benchmark_mapping['b_Code']==portfolio]
        bmk_types = test_benchmark_mapping.columns[1:4].tolist()
        bm_names = test_benchmark_mapping[bmk_types].reset_index().loc[0].tolist()[1:]
        bm_names

        

        test_exp = Portfolio_Exp[Portfolio_Exp['Portfolio']==portfolio]

        test_exp=test_exp.merge(benchmark_mapping[['b_Code','b_Name']], how='left',left_on='Portfolio',right_on='b_Code')
        Bpm_name = test_exp['b_Name'].drop_duplicates().tolist()[0]
        #Drop uneeded columns
        test_exp = test_exp.drop(columns=['Benchmark (%) MSCI World', 'Active Weight (%)','Eff Weight (%)', 'Eff Bmk Weight (%)', 'Eff Active Weight (%)','Delta Adjusted Exposure', 'Notional' ,'P/E', 'P/S', 'P/CE', 'P/BV', 'Dividend Yield (%)','GICS Industry','Inst. Type'])
        

        bm_merge=[]
        for bm,type in list(zip(bm_names,bmk_types)):


            bm_add=Benchmarks[bm][subset_cols].rename(columns={'Portfolio (%)':type + ' Exposure'})
            bm_add[type + ' Name'] = bm
            bm_merge.append(bm_add) 

        # combines position level benchmark data into one dataframe
        bms = bm_merge[0].merge(bm_merge[1],how='outer', on = subset_cols[1:]).merge(bm_merge[2],how='outer', on = subset_cols[1:]).drop_duplicates(subset=subset_cols[1:])
        
        #Removes dates in which the manager does not have data but the benchmark does.
        agg=test_exp.groupby(['b_Name','Date']).sum().reset_index()
        zero_dates=agg[agg['Portfolio (%)']==0]['Date'].tolist()


        test_exp=test_exp[~test_exp['Date'].isin(zero_dates)]
        bms=bms[~bms['Date'].isin(zero_dates)]
        test_exp=[test_exp[test_exp['Look Through Source'].isna()],test_exp[~test_exp['Look Through Source'].isna()]]
        test_exp_LT=test_exp[0].merge(bms, how='outer', on= subset_cols[1:])
        test_exp = pd.concat([test_exp[1],test_exp_LT])

        #fill blank exposure values with 0's
        test_exp['Manager Benchmark Exposure']=test_exp['Manager Benchmark Exposure'].fillna(0)
        test_exp['Region BM Exposure']=test_exp['Region BM Exposure'].fillna(0)
        test_exp['Policy BM Exposure']=test_exp['Policy BM Exposure'].fillna(0)
        test_exp['Portfolio (%)']=test_exp['Portfolio (%)'].fillna(0)
        test_exp['b_Name'] = test_exp['b_Name'].fillna(Bpm_name)
        test_exp['Manager Benchmark Name']=test_exp['Manager Benchmark Name'].fillna(bm_names[0])
        test_exp['Region BM Name']=test_exp['Region BM Name'].fillna(bm_names[2])
        test_exp['Policy BM Name']=test_exp['Policy BM Name'].fillna(bm_names[1])
        test_exp['Portfolio']=test_exp['Portfolio'].fillna(portfolio)
        test_exp['Manager Benchmark Exposure'] = test_exp['Portfolio (%)'] -  test_exp['Manager Benchmark Exposure'] 
        test_exp['Policy BM Exposure'] =  test_exp['Portfolio (%)']-test_exp['Policy BM Exposure'] 
        test_exp['Region BM Exposure'] =  test_exp['Portfolio (%)']-test_exp['Region BM Exposure'] 
        test_exp=test_exp.drop(columns=['b_Code','Mkt Value','Market Capitalization'])
        if bm_names[1] == bm_names[2]:
            test_exp = test_exp.drop(columns = ['Region BM Exposure',	'Region BM Name'])

        elif bm_names[1] == bm_names[2] and bm_names[1]==bm_names[0]:
            test_exp = test_exp.drop(columns = ['Region BM Exposure',	'Region BM Name','Policy BM Exposure','Policy BM Name'])

            


        portfolios.append(test_exp)

    BPM_Exposure_data=pd.concat(portfolios)
    return(BPM_Exposure_data)

def topn(n,Bpm):

    
    """
    Tracks changes for the top N postions for each manager 
    """
  
    b=Bpm[Bpm['Inst. Sub-type']!='Cash'][['Date','b_Name','Asset Name','Portfolio (%)']]
    b=b.groupby(['Date','b_Name','Asset Name']).sum().reset_index()
    b=b.sort_values(['Date','b_Name','Portfolio (%)'],ascending=False).groupby(['Date','b_Name']).head(n).reset_index(drop=True)

    b=b[['Date','b_Name','Asset Name','Portfolio (%)']]

    managers = b['b_Name'].drop_duplicates().tolist()
    dates=b['Date'].drop_duplicates()

    dfs=[]
    subsets=[]
    mo = len(dates)

    fs=[]
    for manager in managers:
        sb = b[b['b_Name']==manager]
        sub_pos = sb.drop_duplicates(subset=['Asset Name'])[['Asset Name','b_Name']]

        dates = b['Date'].drop_duplicates().tolist()
        for date in dates:
            sub_pos['Date']=date
            ch = sb[sb['Date']==date]
            ch=ch.merge(sub_pos,on=['Asset Name','b_Name','Date'],how='outer')
            fs.append(ch)
    b=pd.concat(fs)

    for manager in managers:
        subset = b[b['b_Name']==manager]
        assets = subset['Asset Name'].tolist()
        pos_exposure = subset['Portfolio (%)'].tolist()
        for pos,expo in list(zip(assets,pos_exposure)):
            d=[]
            dates = b['Date'].drop_duplicates().tolist()
            for i in range (len(dates)):
                chunk = subset[subset['Date']==dates[i]]
                chunk_lst = chunk['Asset Name'].tolist()
                exp = chunk['Portfolio (%)'].tolist()
                if pos in chunk_lst:
                    index = chunk_lst.index(pos)
            
                    d.append(exp[index])
                else:
                    d.append('')  
            dfs.append(d)
                
        subsets.append(subset.reset_index(drop=True))    

        j=pd.concat(subsets)
        x=pd.DataFrame(dfs)
        
    cols = b.columns.tolist()+dates
    j=j.reset_index(drop=True)
    t=j.merge(x,how='left',left_index=True,right_index=True)
    t.columns = cols
    t.columns=[str(x).replace('00:00:00','').strip() for x in t.columns.tolist()]

    return(t)

def PCTab(BPM_Exposure_data):

    b=BPM_Exposure_data[(BPM_Exposure_data['Inst. Sub-type']!='Cash')& ~(BPM_Exposure_data['Inst. Sub-type'].isna())]
    LT_drop_keywords = ['redacted']
    b=b[~b['Look Through Source'].isin(LT_drop_keywords)]

    b=b[['Date','b_Name','Asset ID','Asset Name','Portfolio (%)','Holdings','BARRAID']]
    b['Holdings']=b['Holdings'].astype('float64')
    b=b.groupby(['Date','b_Name','BARRAID','Asset Name']).sum().reset_index()


    managers = b['b_Name'].drop_duplicates().tolist()


    dfs=[]
    subsets=[]

    fs=[]
    for manager in managers:
        sb = b[b['b_Name']==manager]
        threshhold = 1.5
        max_exp=sb[['Asset Name','b_Name','BARRAID','Portfolio (%)']].groupby(['BARRAID']).max().reset_index()
        max_assets=max_exp[max_exp['Portfolio (%)']>=threshhold]['BARRAID'].tolist()
        sb=sb[sb['BARRAID'].isin(max_assets)]

        sub_pos = sb.drop_duplicates(subset=['Asset Name'])[['Asset Name','b_Name','BARRAID']]

        dates = b['Date'].drop_duplicates().tolist()
        for date in dates:
            sub_pos['Date']=date
            ch = sb[sb['Date']==date]
            if ch.shape[0]==0:
                next
            else:
                ch=ch.merge(sub_pos,on=['Asset Name','b_Name','Date','BARRAID'],how='outer')
                fs.append(ch)
    b=pd.concat(fs)
    b=b.drop_duplicates()
    b['Portfolio (%)'] = b['Portfolio (%)'].fillna(0)
    b['Holdings'] = b['Holdings'].fillna(0)


    b.index=b['Date']
    b.index.name = None
    b['Prev_shares']=b.groupby(['b_Name','Asset Name','BARRAID'])['Holdings'].shift(periods=1).fillna(0)
    b['Prev_Weight']=b.groupby(['b_Name','Asset Name','BARRAID'])['Portfolio (%)'].shift(periods=1).fillna(0)

    b['Shares_Pct_change'] = round(((b['Holdings']-b['Prev_shares'])/b['Prev_shares'])*100,0)
    b['Weight_Pct_change'] = round(((b['Portfolio (%)']-b['Prev_Weight'])/b['Prev_Weight'])*100,0)

    b['Shares_Pct_change']=b['Shares_Pct_change'].astype(str)
    b['Shares_Pct_change']=b['Shares_Pct_change'] + '%'
    b['Shares_Pct_change']=b['Shares_Pct_change'].str.replace('inf%','New').str.replace('-100.0%','Exit').str.replace('nan%','').str.replace('-0.0%','')

    b['Weight_Pct_change']=b['Weight_Pct_change'].astype(str)
    b['Weight_Pct_change']=b['Weight_Pct_change'] + '%'
    b['Weight_Pct_change']=b['Weight_Pct_change'].str.replace('-inf%','New').str.replace('inf%','New').str.replace('nan%','').str.replace('-0.0%','0.0%').str.replace('-New','New Short')

    [x for x in b['Weight_Pct_change']]


   
    b['Weight_Pct_change']=['--'  if x =='0.0%' else x for x in b['Weight_Pct_change']]
    b['Shares_Pct_change']=['--'  if x =='0.0%' else x for x in b['Shares_Pct_change']]

    b=b.reset_index(drop=True)
    conditions = [b['Shares_Pct_change']=='New',b['Shares_Pct_change']=='Exit']
    choices=[0,1]
    b['Enter/Exit'] = np.select(conditions,choices,default=2)
    return(b)

def FilterPC(PC,date):
    filtered_pc = PC[PC['Date']>=date]
    dfs=[]
    for manager in filtered_pc['b_Name'].drop_duplicates():
        x=filtered_pc[filtered_pc['b_Name']==manager]
        asset_filter=x[x['Portfolio (%)']>=2.5].drop_duplicates(subset=['BARRAID'])['BARRAID'].tolist()
        x=x[x['BARRAID'].isin(asset_filter)]
        dfs.append(x)
    filtered_pc=pd.concat(dfs)


    filtered_pc['3 Month Lookback Exposure'] = filtered_pc.groupby(['b_Name','Asset Name','BARRAID'])['Portfolio (%)'].shift(periods=3)
    filtered_pc['3 Month Lookback Holdings'] = filtered_pc.groupby(['b_Name','Asset Name','BARRAID'])['Holdings'].shift(periods=3)
    filtered_pc['3 Month Weight Delta'] = round(((filtered_pc['Portfolio (%)']-filtered_pc['3 Month Lookback Exposure'])/filtered_pc['3 Month Lookback Exposure'])*100,0)
    filtered_pc['3 Month Holdings Delta'] = round(((filtered_pc['Holdings']-filtered_pc['3 Month Lookback Holdings'])/filtered_pc['3 Month Lookback Holdings'])*100,0)
    filtered_pc=filtered_pc[~((filtered_pc['Portfolio (%)']==0)& (filtered_pc['Enter/Exit']==2))]
    return(filtered_pc)

def CalculateExposureDeltas(col,BPM_Exposure_data):
    Bpm_Tearsheet = BPM_Exposure_data.groupby(['Date', 'b_Name', col]).sum().reset_index()
    Bpm_Tearsheet['Prev_Year_Weight_Abs']=Bpm_Tearsheet.groupby(['b_Name',col])['Portfolio (%)'].shift(periods=12).fillna(0)
    Bpm_Tearsheet['Prev_Year_Weight_Rel']=Bpm_Tearsheet.groupby(['b_Name',col])['Manager Benchmark Exposure'].shift(periods=12).fillna(0)
    Bpm_Tearsheet['YOY Change_Abs'] = Bpm_Tearsheet['Portfolio (%)'] - Bpm_Tearsheet['Prev_Year_Weight_Abs']

    Bpm_Tearsheet['YOY Change_Rel'] = Bpm_Tearsheet['Manager Benchmark Exposure'] - Bpm_Tearsheet['Prev_Year_Weight_Rel']
    Bpm_Tearsheet['3M_Weight_Abs']=Bpm_Tearsheet.groupby(['b_Name',col])['Portfolio (%)'].shift(periods=3).fillna(0)
    Bpm_Tearsheet['3M_Weight_Rel']=Bpm_Tearsheet.groupby(['b_Name',col])['Manager Benchmark Exposure'].shift(periods=3).fillna(0)
    Bpm_Tearsheet['3M Change_Abs'] = Bpm_Tearsheet['Portfolio (%)'] - Bpm_Tearsheet['3M_Weight_Abs']
    Bpm_Tearsheet['3M Change_Rel'] = Bpm_Tearsheet['Manager Benchmark Exposure'] - Bpm_Tearsheet['3M_Weight_Rel']

    Bpm_Tearsheet['1M_Weight_Abs']=Bpm_Tearsheet.groupby(['b_Name',col])['Portfolio (%)'].shift(periods=1).fillna(0)
    Bpm_Tearsheet['1M_Weight_Rel']=Bpm_Tearsheet.groupby(['b_Name',col])['Manager Benchmark Exposure'].shift(periods=1).fillna(0)
    Bpm_Tearsheet['1M Change_Abs'] = Bpm_Tearsheet['Portfolio (%)'] - Bpm_Tearsheet['1M_Weight_Abs']
    Bpm_Tearsheet['1M Change_Rel'] = Bpm_Tearsheet['Manager Benchmark Exposure'] - Bpm_Tearsheet['1M_Weight_Rel']

    Bpm_Tearsheet['6M_Weight_Abs']=Bpm_Tearsheet.groupby(['b_Name',col])['Portfolio (%)'].shift(periods=6).fillna(0)
    Bpm_Tearsheet['6M_Weight_Rel']=Bpm_Tearsheet.groupby(['b_Name',col])['Manager Benchmark Exposure'].shift(periods=6).fillna(0)
    Bpm_Tearsheet['6M Change_Abs'] = Bpm_Tearsheet['Portfolio (%)'] - Bpm_Tearsheet['6M_Weight_Abs']
    Bpm_Tearsheet['6M Change_Rel'] = Bpm_Tearsheet['Manager Benchmark Exposure'] - Bpm_Tearsheet['6M_Weight_Rel']
    Bpm_Tearsheet['col'] = Bpm_Tearsheet[col].fillna('Other')
    Sector_AVG=Bpm_Tearsheet.groupby(['b_Name',col]).mean().reset_index().rename(columns={'Portfolio (%)':'LT Sector AVG ABS','Manager Benchmark Exposure':'LT Sector AVG REL'})[['b_Name',col,	'LT Sector AVG ABS','LT Sector AVG REL'	]].drop_duplicates()

    Bpm_Tearsheet=Bpm_Tearsheet[Bpm_Tearsheet['Date']==Bpm_Tearsheet['Date'].max()]
    Bpm_Tearsheet=Bpm_Tearsheet[[ 'b_Name','Date',col,'Portfolio (%)','Prev_Year_Weight_Abs', 'Prev_Year_Weight_Rel',  'YOY Change_Abs', 'YOY Change_Rel','6M Change_Abs','6M Change_Rel','1M Change_Abs','1M Change_Rel','3M Change_Abs','3M Change_Rel','3M_Weight_Abs','Manager Benchmark Exposure']]

    Bpm_Tearsheet=Bpm_Tearsheet.merge(Sector_AVG,how='left', on = ['b_Name',col]).drop_duplicates()

    return(Bpm_Tearsheet)

def BarraFactorsTE(FACTOR_PATH):

    files_list = os.listdir(FACTOR_PATH)[:-2]
    dfs=[]
    for f in files_list:
        te = pd.read_csv(FACTOR_PATH+'//'+f,header=11).dropna(how='all')
        initial_chunk = pd.read_csv(FACTOR_PATH+'//'+f,nrows=10)
        initial_date = initial_chunk[initial_chunk['Client Name:']=='Analysis Date:'].reset_index().loc[0][2]
        portfolio=initial_chunk[initial_chunk['Client Name:']=='Portfolio:'].reset_index().loc[0][2]

        dates=te[te['Risk Source']=='Analysis Date:']['Parent Node'].tolist()
        dates.insert(0,initial_date)
        te=te.dropna(subset=['TR Contribution']).reset_index(drop=True)
        drop_lst = ['Total','Local Excess','Market Timing','Residual','Common Factor','Risk Indices','Country','Industry','World','Factor Interaction','Risk Source']
        te=te[~te['Risk Source'].isin(drop_lst)].reset_index(drop=True)
        te['Parent Node'] = [x.split('/')[-1] for x in  te['Parent Node']]
        nums= te[te['Risk Source']=='Zambia'].index.tolist()
        te = te[te['Parent Node']!='Total']
        te['Parent Node'] = te['Parent Node'].str.replace('Residual','Specfic')

        nums.insert(0,0)

        dfs_split=[]

        for i in range(len(nums)-1):
            chunk = te.loc[nums[i]:nums[i+1]]
            chunk['Date'] = dates[i]
            chunk['TE Contribution'] = chunk['TE Contribution'].str.replace('%','').astype('float64')
            chunk['Active Exp'] = chunk['Active Exp'].astype('float64')
            total_te = chunk['TE Contribution'].sum()
            chunk['% of TE Contribution'] = (chunk['TE Contribution']/total_te)
            
            chunk['TE Contribution'] = chunk['TE Contribution']/100
            dfs_split.append(chunk)

        te=pd.concat(dfs_split)

        te['Portfolio'] = portfolio
        dfs.append(te)

    te=pd.concat(dfs)
    return(te)

def CalcTEAvgs(te):
    ranges=[1,3,6,12]
    cols = ['Active Exp','% of TE Contribution']
    for col in cols:
        for ar in ranges:
            te[str(ar)+'M Delta' +' ' +col]=te[col] - te.groupby(['Portfolio','Risk Source'])[col].shift(periods=ar)
            # change to risk source
    avgs = te[['Portfolio','Risk Source','Active Exp','% of TE Contribution']].groupby(['Portfolio','Risk Source']).mean().reset_index()
    avgs=avgs.rename(columns={'Active Exp':'Active Exp LT AVG','% of TE Contribution':'% of TE Contribution LT AVG'})

    # Have to calculate topline avgs for all of the groupings because powerBI will sum them instead
    topline_avgs = te[['Portfolio','Parent Node','Active Exp','% of TE Contribution']].groupby(['Portfolio','Parent Node']).mean().reset_index()
    topline_avgs=topline_avgs.rename(columns={'Active Exp':'Active Exp LT AVG_topline','% of TE Contribution':'% of TE Contribution LT AVG_topline'})

    te=te.merge(avgs, how='left', on=['Portfolio','Risk Source'])
    te=te.merge(topline_avgs, how='left', on=['Portfolio','Parent Node'])
    #te = te[(te['Active Exp']!=0.00) & (te['TE Contribution']!=0.00 )]
    return(te)

if __name__ ==  '__main__':

    PATHS = pd.read_csv("redacted")
    PATHS={x:y for x,y in list(zip(PATHS['Name'],PATHS['Path']))}
  


    Portfolio_Exp=pd.concat([TransformPositionData(PATHS['POS_PATH'],x,early_date) for x in os.listdir(PATHS['POS_PATH'])])
    RenameBMKS(PATHS['BMK_PATH'])
    Benchmarks = CalcBenchmarks(PATHS['BMK_PATH'])
    benchmark_mapping=MapBenchmarks(PATHS['BMK_MAP_PATH'],PATHS['MGR_CODE_PATH'],PATHS['BMK_NAME_PATH'])



    
    Modeled_Positions = pd.read_csv(PATHS['BPM_POS_OUTPUT_PATH'])
    Top_Ten=topn(10,Modeled_Positions)
    PC = PCTab(Modeled_Positions)
    Filtered_PC = FilterPC(PC)
    ExposureDeltas_region= CalculateExposureDeltas('Region',Modeled_Positions)
    ExposureDeltas_sector= CalculateExposureDeltas('GICS Sector',Modeled_Positions)
    Barra_te = BarraFactorsTE(PATHS['FACTOR_PATH'])

    Modeled_Positions.to_csv(PATHS['BPM_POS_OUTPUT_PATH'],index=False)
    Top_Ten.to_csv(PATHS['TOPN_PATH'],index=False)
    PC.to_csv(PATHS['PC_PATH'],index=False)
    Filtered_PC.to_csv(PATHS['F_PC_PATH'],index=False)
    ExposureDeltas_region.to_csv(PATHS['EXP_DELTA_REGION'],index=False)
    ExposureDeltas_sector.to_csv(PATHS['EXP_DETA_SECTOR'],index=False)
    Barra_te.to_csv(PATHS['TE_FACTORS'],index=False)

    Modeled_Positions.to_parquet('redacted')
    