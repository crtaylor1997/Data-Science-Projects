#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import statistics
import numpy as np
#Uses Levenshtein distance formula to match column names that are not exactly the same.
dealcloud_loan=pd.read_csv('Loan Import Template.csv')
loans=pd.read_excel('ML12 Red Data Tape 11.8.2021.xlsm')
loans=loans.dropna(axis=0,how='all')
loans.columns=loans.columns.str.replace(r'\n', '', regex=True)

list1 = dealcloud_loan.columns.tolist()
list2 = loans.columns.tolist()
  
mat1=[]
mat2 = []
p = []
# taking the threshold as 80
threshold = 80
  
# iterating through list1 to extract
# it's closest match from list2
for i in list2:
    x = process.extractOne(i,list1, scorer=fuzz.token_set_ratio)
        
    y = process.extractOne(i,list1, scorer=fuzz.partial_token_set_ratio)
    
    print(i,x,y)
    a=(0.8*x[1]+0.2*y[1])
    if (a>threshold):
             mat1.append((i,(x[0],a)))
print(mat1) 

for j in mat1:
    if j[1][1] >= threshold:
        p.append(j[0])
    mat2.append(",".join(p))
    p = []
mat1 = [list(x) for x in mat1]
mat1=[[x[0],x[1][0],x[1][1]] for x in mat1]
print(mat1)

df = pd.DataFrame(mat1, columns=['ML Column', 'DC Column', 'Score'])
df
#Saves the Column Matches to a CSV file.
df.to_csv('Column Matches ML12 Loan.csv')


# In[58]:


import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import statistics
import numpy as np

columns=pd.read_csv('Column Matches ML Property.csv')
dealcloud=columns['DC Column'].tolist()
ML=columns['ML Column'].tolist()
matches = dict(zip(ML, dealcloud))
ML=pd.read_excel('ML12 Datatpe.xlsx',header=1)
ML=ML.rename(columns=matches).transpose()
Property_Import_Template=pd.read_csv('Property Import Template.csv').transpose()
ML_DealCloud=Property_Import_Template.merge(ML,how='left',left_index=True,right_index=True).transpose()
ML_DealCloud['New Construction Related?']='No'
ML_DealCloud['Loan Type']='Tax-Exempt'
ML_DealCloud['Subordinate Debt Flag']=np.where(ML_DealCloud['Subordinate Debt Flag']=='N','No','Yes')
ML_DealCloud['Deal']='ML12'
ML_DealCloud['Securitization']='ML12 - Freddie Mac'
#Splits a list of numbers seperated by Semicolons and Sums them.

ML_DealCloud['Subordinate Debt Amount']=[x.split(';') if type(x) == str else x for x in ML_DealCloud['Subordinate Debt Amount'] ]
ML_DealCloud['Subordinate Debt Amount']=[sum(map(float,map(lambda y: y.replace(',',''), x))) if type(x) == list else x for x in ML_DealCloud['Subordinate Debt Amount'].tolist()]
# Create a Pandas Excel writer using XlsxWriter as the engine.
writer = pd.ExcelWriter('Loan Import ML12.xlsx', engine='xlsxwriter')

# Write the dataframe data to XlsxWriter. Turn off the default header and
# index and skip one row to allow us to insert a user defined header.
ML_DealCloud.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)

# Get the xlsxwriter workbook and worksheet objects.
workbook = writer.book
worksheet = writer.sheets['Sheet1']

# Get the dimensions of the dataframe.
(max_row, max_col) = ML_DealCloud.shape

# Create a list of column headers, to use in add_table().
column_settings = [{'header': column} for column in ML_DealCloud.columns]

# Add the Excel table structure. Pandas will add the data.
worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

# Make the columns wider for clarity.
worksheet.set_column(0, max_col - 1, 12)

# Close the Pandas Excel writer and output the Excel file.
writer.save()



    #Standardizes Names of Companies to Avoid loading duplicate entries to the database.
def ColesceCompanies(company,column_name):
    #removes special characters, items in parenthesis and extraneous phrases.
    company[column_name]=[x[0] for x in company[column_name].str.split(';',1)]
    company[column_name]=[x[0] for x in company[column_name].str.split(',',1)]
    company[column_name]=company[column_name].str.replace(r'([^a-zA-Z\s]+?)',"",regex=True)
    company[column_name]=company[column_name].str.replace('.',"")
    company[column_name]=company[column_name].str.replace(' LLC',"")
    company[column_name]=company[column_name].str.replace(' LLP',"")
    company[column_name]=company[column_name].str.replace('  '," ",regex=True)
    company[column_name]=company[column_name].str.strip()
    Companies = company[column_name].tolist()
    
    for i in range (len(Companies)):
        if(i!=0):
            if ((Companies[i].lower() in Companies[i-1].lower()) or (Companies)[i-1].lower() in Companies[i].lower()):
                Companies[i]=Companies[i-1]
            elif(fuzz.ratio(Companies[i],Companies[i-1])>80):
                Companies[i]=Companies[i-1]
           
    company[column_name]=Companies
    

ColesceCompanies(ML_DealCloud,'Sponsor')
ColesceCompanies(ML_DealCloud,'Originator')
ColesceCompanies(ML_DealCloud,'Tax Credit Investor')
DealCloudFormat('Loan Import ML12.xlsx', ML_DealCloud)



import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import statistics
import numpy as np
sub=pd.read_csv('C:\\Users\\christian.taylor\\Dealcloud\\ASR\\Subsides_ML.csv')
sub=sub[['Property Name','Description of Regulatory Agreement and Rental Subsidy','HAP Contract Expiration Date','Rental Subsidy Type']]
p=pd.read_excel('Property_Import_ML.xlsx')
p['Property Name']=p['Property Name'].str.strip()
sub['Property Name']=sub['Property Name'].str.strip()



fa=pd.read_excel('Property Import CalHFANFA.xlsx')
fa_sponsors = pd.DataFrame(fa['Tax Credit Syndicator Name'].str.strip().drop_duplicates())
fa_sponsors=fa_sponsors.rename(columns={'Tax Credit Syndicator Name':'Company Name'})
fa_sponsors['Type'] = 'Investor'
fa_sponsors['Sub-Type']='Tax Credit Syndicator'
c=pd.read_csv('Company Import Template.csv')
FA_Sponsors=c.transpose().merge(fa_sponsors.transpose(),how='left',left_index=True,right_index=True).transpose()
DealCloudFormat('FA_Tax Credit Syndicators.xlsx',FA_Sponsors)
FA_Sponsors






