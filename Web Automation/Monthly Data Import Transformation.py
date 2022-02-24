#!/usr/bin/env python
# coding: utf-8

# In[2]:





# In[5]:


import pandas as pd
from datetime import datetime
from fuzzywuzzy import fuzz
import re
loan=pd.read_excel('S://DealCloud//Data Export//Loan.xlsx')
prop=pd.read_excel('S://DealCloud//Data Export//Property.xlsx')
columns=pd.read_excel('Column Matches CREFC.xlsx')
dealcloud=columns['Dealcloud'].tolist()
CREFC=columns['CREFC'].tolist()
matches = dict(zip(CREFC, dealcloud))

CREFC=pd.read_excel('CalHFA 2019-2 Tracker.xlsx',sheet_name='12.20.21',header=1)


CREFC.columns=[x.replace('_',' ') for x in CREFC.columns.tolist()]
CREFC = CREFC = CREFC[CREFC['Loan ID'].notna()]
loans = CREFC['Property Name'].tolist()
#print(loans)
dc_loans=loan['Loan Name (Calc)'].tolist()
indices=[]
for i in range (len(loans)):
    for dc_loan in dc_loans:
        if (fuzz.token_set_ratio(loans[i],dc_loan)>85):
            #print(loans[i] + ' ' + dc_loan)
            loans[i] = dc_loan
            #print(loans[i] + ' ' + dc_loan)
    
CREFC['Property Name']=loans
securitization = loan[loan['Loan Name (Calc)']==CREFC['Property Name'].loc[0]]['Securitization'].tolist()[0]
deal = loan[loan['Loan Name (Calc)']==CREFC['Property Name'].loc[0]]['Deal'].tolist()[0]
properties = prop[prop['Loan'].isin(CREFC['Property Name'])][['Name','Loan']].rename(columns={'Name':'Property'}).drop_duplicates(subset='Loan')
print(CREFC)
CREFC=CREFC.merge(properties, how='left',left_on='Property Name',right_on='Loan')

CREFC=CREFC.rename(columns=matches).transpose()
template=pd.read_excel('Monthly Servicing Record Import Template.xlsx').transpose()
CREFC_DealCloud=template.merge(CREFC,how='left',left_index=True,right_index=True).transpose()
CREFC_DealCloud['Deal']=deal
CREFC_DealCloud['Securitization']=securitization


date_columns=[x for x in CREFC_DealCloud.columns.tolist() if 'Date' in x]
for column in date_columns:
    CREFC_DealCloud[column]=[datetime.strptime(str(x)[2:].split('.')[0], '%y%m%d')  if 'nan' not in str(x)  else '' for x in CREFC_DealCloud[column].tolist()]
    
CREFC_DealCloud = CREFC_DealCloud.iloc[CREFC_DealCloud.isnull().sum(1).sort_values(ascending=True).index].set_index('Prospectus Loan ID')

df=CREFC_DealCloud.loc[CREFC_DealCloud.loc[CREFC_DealCloud.index.duplicated()].index.tolist()].fillna(0)
date=df[date_columns]
date=date[date['Current Occupancy Date']!=0]
dups=df.groupby(df.index).sum().replace(0,'').merge(date,left_index=True,right_index=True)
CREFC_DealCloud=pd.concat([dups,CREFC_DealCloud.drop(CREFC_DealCloud.loc[CREFC_DealCloud.index.duplicated()].index.tolist())]).reset_index()


writer = pd.ExcelWriter('CREFC DealCloud Import.xlsx', engine='xlsxwriter')

# Write the dataframe data to XlsxWriter. Turn off the default header and
# index and skip one row to allow us to insert a user defined header.
CREFC_DealCloud.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)

# Get the xlsxwriter workbook and worksheet objects.
workbook = writer.book
worksheet = writer.sheets['Sheet1']

# Get the dimensions of the dataframe.
(max_row, max_col) = CREFC_DealCloud.shape

# Create a list of column headers, to use in add_table().
column_settings = [{'header': column} for column in CREFC_DealCloud.columns]

# Add the Excel table structure. Pandas will add the data.
worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

# Make the columns wider for clarity.
worksheet.set_column(0, max_col - 1, 12)

# Close the Pandas Excel writer and output the Excel file.
writer.save()


# In[21]:





# In[196]:


CREFC_DealCloud = CREFC_DealCloud.iloc[CREFC_DealCloud.isnull().sum(1).sort_values(ascending=True).index].set_index('Prospectus Loan ID')
CREFC_DealCloud.groupby(CREFC_DealCloud.index).bfill()


# In[6]:


df=CREFC_DealCloud.loc[CREFC_DealCloud.loc[CREFC_DealCloud.index.duplicated()].index.tolist()].fillna(0)
date=df[date_columns]
date=date[date['Current Occupancy Date']!=0]
dups=df.groupby(df.index).sum().replace(0,'').merge(date,left_index=True,right_index=True)
pd.concat([dups,CREFC_DealCloud.drop(CREFC_DealCloud.loc[CREFC_DealCloud.index.duplicated()].index.tolist())])


# In[25]:


CREFC.head(50)


# In[28]:


properties.drop_duplicates(subset='Loan')


# In[ ]:




