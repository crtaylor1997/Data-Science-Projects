#!/usr/bin/env python
# coding: utf-8

# In[17]:

#This Python Script was designed to Automate data exctaction from a CRM service that did not support a useable API. 
#It works with the Selenium package which using an automated window of Chrome running  invisibly in the background. 
#It uses the site's UI to download nesseary files, rename them and place them in a specific file path to be used for later reports and data analysis.


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
import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler
import os
import os
import shutil

def ExportData(count):   
    # Connect to DataSource
    PATH = 'C:\\Users\\chromedriver.exe'
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('headless')
    options.add_argument('--window-size=1920,1080')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(PATH,options=options)
    params = {'behavior': 'allow', 'downloadPath': 'C:\\Users\\Downloads'}
    downloadPath= 'C:\\Users\\\\Downloads'
    driver.get('https://app.dealcloud.com/Account/Login')
    USERNAME = ""
    PASSWORD = ""
    email=driver.find_element_by_id("Email")
    email.send_keys(USERNAME)
    page_source=driver.page_source
    soup = bs(page_source, 'html.parser')
    time.sleep(3)
    driver.find_element_by_css_selector("input[type='button']").click()
    time.sleep(3)
    password=driver.find_element_by_id("Password")
    password.send_keys(PASSWORD)
    driver.find_element_by_css_selector("input[type='submit']").click()
    
    #Deal
    driver.get('https://systima.dealcloud.com/portal/pages/28952/reports/-37771')
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div/section/section/div[1]/div/div/div/div/div/div/div/div/div[1]/div[2]/div/button")))
    element.click()
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[30]/div/div/div[4]")))
    element.click()
    driver.execute_cdp_cmd('Page.setDownloadBehavior', params)
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[30]/div/div/div[1]")))
    element.click()
    time.sleep(5)
    
    
    #Loan
    driver.get('https://systima.dealcloud.com/portal/pages/28952/reports/-37770')
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div/section/section/div[1]/div/div/div/div/div/div/div/div/div[1]/div[2]/div/button")))
    element.click()
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[30]/div/div/div[4]")))
    element.click()
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[30]/div/div/div[1]")))
    element.click()
    time.sleep(30)
    
    
    
    #Property
    driver.get('https://systima.dealcloud.com/portal/pages/28952/reports/-41085')
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div/section/section/div[1]/div/div/div/div/div/div/div/div/div[1]/div[2]/div/button")))
    element.click()
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[30]/div/div/div[4]")))
    element.click()
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[30]/div/div/div[1]")))
    element.click()
    time.sleep(20)
    
    
    #Servicing Record
    driver.get('https://systima.dealcloud.com/portal/pages/28952/reports/-51561')
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div/section/section/div[1]/div/div/div/div/div/div/div/div/div[1]/div[2]/div/button")))
    element.click()
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[30]/div/div/div[4]")))
    element.click()
    element = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.XPATH, "/html/body/div[30]/div/div/div[1]")))
    element.click()
    
    # Verify files were correctly downloaded and move to data warehouse for processing
    
    files = sorted(os.listdir(downloadPath), key=lambda x: os.path.getctime(os.path.join(downloadPath, x)))
    dealcloud_files=files[-4:]
   
    for file in dealcloud_files:
        filename=file.split('_')[0]+'.xlsx'
        shutil.move('C:/Users/christian.taylor/Downloads/'+file,'C:/Users/christian.taylor/Dealcloud/Dealcloud Export/'+filename)
        t = (os.path.getmtime('C:/Users/christian.taylor/Dealcloud/Dealcloud Export/'+filename))
        if ((t >= time.time()-300)&(filename in ['Loan.xlsx', 'MonthlyServicingRecord.xlsx', 'Deal.xlsx', 'Property.xlsx'])):
            print(filename+' Downloaded Successfully')
            shutil.move('C:/Users/christian.taylor/Dealcloud/Dealcloud Export/'+filename,'S:/DealCloud/Data Export/'+filename)
        else:
            print(filename+' Download failed')
            if count < 4:
                print('Attempting Download Again...'+ ' Attempt:'+ str(count))
                count=count+1
                ExportData(count)
            else:
                print('Download Attempt failed three times. Quitting...')
                break
            break

count=1
ExportData(count)   









 


# In[ ]:





# In[ ]:




