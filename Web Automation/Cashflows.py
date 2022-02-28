#!/usr/bin/env python
# coding: utf-8

# In[2]:



# This Program uses selenium to automate Chrome to navigate to a website, use a drop-down menu to extract navigate to links showing an unexportable table with raw data.
# Using Beautiful Soup and the data link, these tables are scraped and saved into CSV files that can be manipulated using Pandas and MS Excel.
	





from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium import webdriver
from bs4 import BeautifulSoup as bs
from bs4 import SoupStrainer
import re as re
from selenium.webdriver.support.ui import Select 
import time
import pandas as pd
from selenium.webdriver.support.select import Select
import numpy as np
from pandas import ExcelWriter
import itertools as it


# In[1]:

#logs into website

PATH = 'C:\\Users\\christian.taylor\\northshore_Scripts\\chromedriver.exe'

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument("start-maximized")
driver = webdriver.Chrome(PATH,chrome_options=chrome_options)
driver.get('https://systima.northshoresystems.com/NorthshoreWeb/Default.aspx')
USERNAME = ("")
PASSWORD = ("")
option = driver.find_element_by_id('TB_UserName')
script=option.get_attribute('onBlur')
print(script)
driver.execute_script(script)
#option.send_keys(USERNAME)


# In[13]:

#This fucntion find elements of the webpage where data is stored, scrapes it and stores it in a Pandas dataframe that is saved as a CSV

def formatCashFlows(link,deal):    
    driver.get(link)
    page_source=driver.page_source
    soup = bs(page_source, 'html.parser')
    t=soup.find("table",id="FpSpread1_viewport2")
    rows_index_lst=[]

    trs=t.find_all('tr')
    for tr in trs:   
        row=[]
        tds = tr.find_all('td')
        #print(tr.text)
        for td in tds:
            row.append(td.text)
        #print(td.text)
        rows_index_lst.append(row)
       # Locates the data Tables
    
    t2=soup.find("table",id="FpSpread1_viewport")    
    rows_lst=[]
    trs2=t2.find_all('tr')
    for tr in trs2: 
        fpkey=tr.get('fpkey')
        row=[]
        tds2 = tr.find_all('td')
        #print(tr.text)
        for td in tds2:
            tdclass = td.get('class')
            if(tdclass[0] == 'CellReadonlyTextRight')|(tdclass[0] == 'CellReadonlyTextRight2')|(tdclass[0] == 'CellEditNumberCF') :   
                row.append(td.text)
    #print(len(row))
        #print(td.text)
        rows_lst.append(row)
    t3=soup.find("table",id="FpSpread1_viewport1")
    trs3=t3.find_all('tr')
    for tr in trs3:
        fpkey=tr.get('fpkey')
    #print(fpkey)
        if (fpkey == "2"):
            col_header=[]
            tds3 = tr.find_all('td')
            for td in tds3:
                col_header.append(td.text)
        elif (fpkey == "1"):
            years=[]
            tds3a = tr.find_all('td')
            for td in tds3a:
                years.append(td.text)
        elif (fpkey == "0"):
            names=[]
            tds3b = tr.find_all('td')
            for td in tds3b:
                tdclass2 = td.get('class')
            #print(tdclass2)
            #tdclass2.type
            #print(tdclass)
                if(tdclass2==['s0s2']):
                #t=td.find('table')     
                    select=td.find("select")
                    # Extract text from elements
                    opt=select.find("option",selected=True).text
                    names.append(opt)
                
        
        
            #print(td.text)
            
    # Transforms Data from raw text elements to those that can be stored in a Pandas dataframe.
    
    col_header = [x for x in col_header if(x != '') and (x != '\xa0')]
    # Drops spaces and empty elements
    years = [x for x in years if('\n' not in x) and ('\xa0' not in x) and (x != '') and (x != 'YE') and (x != 'Annualized')]
    rows_index_lst = [sub[0] for sub in rows_index_lst ]
#print(col_header)
#print(rows_lst)
#print(years)

    segments=[list(g) for k, g in it.groupby(col_header, lambda x: x=='% of EGI') if not k]
    for segment in segments:segment.append('% of EGI')
    arrays=[years,segments,names]
    lst=[]
    
    # create a multindexed dataframe with three header columns
    
    tuples = list(zip(*arrays))
    for t in tuples:
        for e in range(len(t[1])):
            lst.append([t[0],t[2],t[1][e]])
    
            
    lst_tup = [tuple((elem[0],elem[1],elem[2])) for elem in lst]        
    index = pd.MultiIndex.from_tuples(lst_tup, names=["Operating Period","Type","Measure"])   
        
        
    cash=pd.DataFrame(rows_lst,columns=index)
    cash.insert(0,'Category',rows_index_lst)
    cash=cash.fillna(value=np.nan)
    cash=cash.replace(r'^\s*$', np.nan, regex=True) 
    cash=cash.dropna(axis=1,how ='all')
    cash=cash.dropna(axis=0,how ='all')
#print(years)
#print(col_header)
    deal=deal.replace('/',"")
    cash['Deal Name']= deal

#del tuple

#Creates final dataframe and saves it to CSV file.

    cash.drop(columns={x for x in index if ('Adj' in x) | ('Adjusted' in x)}, inplace=True)
    cash.to_csv(deal+'.csv')
    #cash


# In[5]:

# Drops corrupted or missing data

deals=[]
deal_codes=[]
driver.get('https://systima.northshoresystems.com/NorthshoreWeb/NS/Header/ModulePipeline.aspx')
driver.switch_to_default_content()
ele = driver.find_element_by_id("ttop")
driver.switch_to.frame(ele)
y=driver.find_element_by_id('DL_Loan')
for option in y.find_elements_by_tag_name('option'):
    deals.append(option.text)
    deal_codes.append(option.get_attribute('value'))
deals.remove('Marshall - Marshall Field Garden Apartments')
deals.remove('CCRC-GEAHI - GEAHI Old Town Kern LP')
deals.remove('Florida Port T.E. - Florida Portfolio (Tax-exempt)*')
deals.remove('Florida Tax. - Florida Portfolio (Taxable)*')
deals.remove('ML06-666 Ellis - 666 Ellis Street')
deals.remove('Vista - Vista Shadow Mountain Apts')
deals.remove('KK 102219 LN 01')


# In[12]:

# Navigates website and extracts target links containing needed data. 

def getCashFlowLink(link, deal):
    driver.get(link)
    ele = driver.find_element_by_id("ttop")
    driver.switch_to.frame(ele)
    driver.find_element_by_xpath("//select[@name='DL_Loan']/option[text()=""'" + deal+"'"''"]").click()
    driver.switch_to_default_content()
    ele = driver.find_element_by_id("flow")
    driver.switch_to.frame(ele)
    e=driver.find_element_by_id("tr_menu_grp_6")
    option = e.find_element_by_id('td_menu_item_13')
    script=""
    script=option.get_attribute('onClick')
    time.sleep(3)
    driver.execute_script(script)
    e=""
    option=""
    driver.switch_to_default_content()
    f=driver.find_element_by_id('main')
    driver.switch_to.frame(f)
    elem= driver.find_element_by_id("form1")
    z=elem.get_attribute('action')
    print(deal+', '+z)
    rent_links.append(z)
    return (z)


# In[10]:


deals[463]


# In[19]:


rent_links=[]
base_link = 'https://systima.northshoresystems.com/NorthshoreWeb/NS/Header/ModulePipeline.aspx'

# Loops through all 500 items in the system, gets their respective links, scrapes the data, and saves the files.
for deal in deals:
    formatCashFlows(getCashFlowLink(base_link, deal),deal)


# In[96]:


T


# In[88]:


import pandas as pd
import os
import csv
import glob 
path= "C:\\Users\\christian.taylor\\Northshore Cashflows"
writer = pd.ExcelWriter('Cashflows.xlsx', engine='xlsxwriter')
all_files = glob.glob(os.path.join(path, "*.csv"))
for f in all_files:
    df = pd.read_csv(f)
    sheet_name=' '.join(os.path.basename(f).split('-')[:2]).rstrip()
    df.to_excel(writer, sheet_name=sheet_name)

writer.save()

' '.join(os.path.basename(f).split('-')[2]).rstrip()


# In[7]:


columns=pd.read_excel('Column Matches Cashflow.xlsx')
dealcloud=columns['DC'].tolist()
ML=columns['NS'].tolist()
matches = dict(zip(ML, dealcloud))
xls = pd.ExcelFile('Cashflows.xlsx')
T=pd.read_csv('Cashflow Underwriting Record Import Template.csv')
cashflows=[]
for sheet in xls.sheet_names:
    c= pd.read_excel('Cashflows.xlsx',sheet)
    deal=c['Deal Name'][4]
    c['Category'][0]='Type'
    c['Category'][1]='Measure'
    c=c.drop(columns=['Unnamed: 0','Operating Period'])
    c.index=c['Catagory']
    c=c.transpose()[c.transpose()['Measure']=='Amount'].transpose()
    c.columns=[x[0]for x in c.columns.str.split('.')]
    c=c.transpose()
    c['Year']=[x[-4:] for x in c.index]
    c['Loan']=deal.split('-')[2]
    c=c.rename(columns=matches)
    c= c.loc[:,~c.columns.duplicated()]
    c=T.transpose().merge(c.transpose(), how='left',left_index=True,right_index=True).transpose()
    c=c.reset_index(drop=True)
#c.to_csv('c.csv')
    cashflows.append(c)
    
C=pd.concat(cashflows)


# In[57]:


import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import statistics
import numpy as np
dealcloud_loan=pd.read_csv('Cashflow Underwriting Record Import Template.csv')
loans=c
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

df = pd.DataFrame(mat1, columns=['NSS Column', 'DC Column', 'Score'])
df
df.to_csv('Column Matches Cashflow Property.csv')


# In[4]:


columns=pd.read_excel('Column Matches Cashflow.xlsx')
dealcloud=columns['DC'].tolist()
ML=columns['NS'].tolist()
matches = dict(zip(ML, dealcloud))


# In[5]:


matches


# In[6]:


C.dropna(how='all',axis=1).columns.tolist()


# In[8]:


C=C.reset_index(drop=True)
def DealCloudFormat(File_Name,C):
    writer = pd.ExcelWriter(File_Name, engine='xlsxwriter')

    # Write the dataframe data to XlsxWriter. Turn off the default header and
    # index and skip one row to allow us to insert a user defined header.
    C.to_excel(writer, sheet_name='Sheet1', startrow=1, header=False, index=False)

    # Get the xlsxwriter workbook and worksheet objects.
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']

    # Get the dimensions of the dataframe.
    (max_row, max_col) = C.shape

    # Create a list of column headers, to use in add_table().
    column_settings = [{'header': column} for column in C.columns]

    # Add the Excel table structure. Pandas will add the data.
    worksheet.add_table(0, 0, max_row, max_col - 1, {'columns': column_settings})

    # Make the columns wider for clarity.
    worksheet.set_column(0, max_col - 1, 12)

# Close the Pandas Excel writer and output the Excel file.
    writer.save()
DealCloudFormat('Cashflow Import.xlsx',C)


# In[154]:


c.columns[c.columns.duplicated()]


# In[155]:


len(c.columns.duplicated())


# In[139]:


c['Loan']


# In[140]:


c['Year']


# In[161]:


len(T.columns)


# In[162]:


len(C.columns)


# In[163]:


C.columns.tolist()


# In[164]:


C['Total Number of Units']


# In[29]:


loan=pd.read_excel('S://DealCloud//Data Export//Loan.xlsx')
prop=pd.read_excel('S://DealCloud//Data Export//Property.xlsx')
props=[]
CF_loans = [x.strip() for x  in C['Loan'].tolist()]
for CF_loan in CF_loans:
    if CF_loan.lower() in prop['Loan'].str.lower().tolist():
        props.append(prop.set_index('Loan').loc[CF_loan]['Name'])
    else:
        props.append('')


# In[28]:


props


# In[18]:


import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import statistics
import numpy as np

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
df.to_csv('Column Matches ML12 Loan.csv')


# In[31]:


C['Record Name'].drop_duplicates()


# In[ ]:




