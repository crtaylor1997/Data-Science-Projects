#!/usr/bin/env python
# coding: utf-8

# In[83]:





# In[345]:


#This Python script transforms a routinely received dataset, that contains information on the credit ratings
# of different securities. The initial dataset contains several ratings per security based on
#the tranche and rating company. This data is stored as a block of text. The script takes these text blocks,
#tokenizes them into lists with regex, and splits the lists into different rows based on the tranche.
# This format contains only one piece of information per cell, so it is easier to upload and manipulate
#in a relational database.

 

import pandas as pd
STACR=pd.read_csv('STACR.csv').transpose()
STACR=STACR.reset_index()
STACR.columns=STACR.loc[0]

STACR['Bond Issuance']=STACR['Bond Issuance'].str.replace(r'\n', '|', regex=True)
STACR['Spread']=STACR['Spread'].str.replace(r'\n', '|', regex=True)
STACR['Rating1']=STACR['Rating1'].str.replace(r'\n', '|', regex=True)
STACR['Initial Credit Enhancement']=STACR['Initial Credit Enhancement'].str.replace(r'\n', '|', regex=True)
STACR_t=STACR[['Deal','Bond Issuance','Spread','Rating1','Initial Credit Enhancement','Closing Date','Rater','Total Bond Issuance','Aggregation Period*','Minimum Credit Enhancement','Benchmark']].drop(0)
STACR_t['Bond Issuance']=STACR_t['Bond Issuance'].str.split('|').tolist()

STACR_t['Initial Credit Enhancement']=STACR_t['Initial Credit Enhancement'].str.split('|').tolist()
STACR_t['Bond Issuance']=[x.insert(0,'A-H: ') or x for x in STACR_t['Bond Issuance'].tolist()]
STACR_t['Spread']=STACR_t['Spread'].str.split('|')
STACR_t['Spread']=[x.insert(0,'A-H: ') or x for x in STACR_t['Spread'].tolist()]
STACR_t['Rating1']=STACR_t['Rating1'].str.split('|')
STACR_t['Rating1']=[x.insert(0,'A-H: / ') or x for x in STACR_t['Rating1'].tolist()]
STACR_t=STACR_t.set_index(['Deal','Closing Date','Rater','Total Bond Issuance','Aggregation Period*','Minimum Credit Enhancement','Benchmark']).apply(pd.Series.explode).reset_index()

STACR_t[['Tranche','Bond Issuance']] = STACR_t['Bond Issuance'].str.split(':',expand=True)
STACR_t[['Tranche','Initial Credit Enhancement']] = STACR_t['Initial Credit Enhancement'].str.split(':',expand=True)
STACR_t[['Tranche','Spread']] = STACR_t['Spread'].str.split(':',expand=True)
STACR_t[['Tranche','Rating1']] = STACR_t['Rating1'].str.split(':',expand=True)
STACR_t[['Rating1','Rating2']] = STACR_t['Rating1'].str.split('/',expand=True)
STACR_t[['Rater1','Rater2']] = STACR_t['Rater'].str.split('/',expand=True)
STACR_t=STACR_t.drop(['Rater'], axis=1)
cols=['Deal','Tranche','Total Bond Issuance','Benchmark','Closing Date','Aggregation Period*','Bond Issuance','Spread','Minimum Credit Enhancement','Initial Credit Enhancement','Rater1','Rater2','Rating1','Rating2']
STACR_t = STACR_t[cols]


# In[347]:


STACR_t.to_csv('STACR_reformatted.csv')


# In[346]:


STACR_t


# In[ ]:




