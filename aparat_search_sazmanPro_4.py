print('بسمه الله الرحمن الرحیم')
print('salam bar abbas daneshgar')
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup as b
import pandas as pd
import numpy as np
from bs2json import bs2json
import re
import json
import copy
from copy import deepcopy
import requests
from collections import OrderedDict
from iteration_utilities import unique_everseen
import time
import itertools
import pyodbc 
from sqlalchemy import create_engine
from pyodbc import *
import time
import datetime
import psycopg2
import pandas.io.sql as psql

# import time
# import datetime

# start = time.time() 
###########################
# connection = psycopg2.connect(user="postgres",
#                             password="12344321",
#                             host="10.32.141.17",
#                             port="5432",
#                             database="SazmanPro")
# cursor = connection.cursor()

# input0= psql.read_sql("select * from public.varzesh26101400_1", connection)
# print(len(input0))
input0= pd.read_excel(r'C:\Users\PC\Downloads\aparat2.xlsx')
# input0= pd.read_excel('/home/armin/imdbproject/crawler_input/aparat_search.xlsx')
input_name_list = list(itertools.chain(*input0.iloc[:, [0]].values.tolist()))
# input0= pd.read_excel ('/home/armin/imdbproject/agrigatorinput/tiva_out_total.xlsx')

print(input_name_list)

# input_content_name='سمت خدا'
driver = webdriver.Chrome(r'D:\python\source code\aparat_crawler\chromedriver (2).exe')

data_frame_1 = pd.DataFrame()

ii = 0

for  ii in range(len(input_name_list)):
    try:
        input_content_name=input_name_list[ii]
        print(input_content_name)
        driver.get('https://www.aparat.com/')
        converter = bs2json()
        time.sleep(3)
        driver.get('https://www.aparat.com/search/'+input_content_name+'?uploadedIn=year') 
        SCROLL_PAUSE_TIME = 4
        last_height = driver.execute_script("return document.documentElement.scrollHeight")
        start1=time.time()
        while True:
            # Scroll down to bottom
            driver.execute_script("window.scrollTo(0,document.documentElement.scrollHeight);")
        # Wait to load page
            time.sleep(SCROLL_PAUSE_TIME)
        # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                print("break")
                break
            last_height = new_height
            scroll_time=time.time()-start1
            print(scroll_time)
        # if scroll_time >20:
        #     print('scroll time finshed')
        #     break
        fb= WebDriverWait(driver,30).until(EC.presence_of_element_located((By.XPATH, '//*[@id="list1"]/div/div')))
    # fb_dur= WebDriverWait(driver,30).until(EC.presence_of_element_located((By.XPATH, '//*[@id="thumb32600659"]/div/div[1]/a/div[2]')))
    # //*[@id="thumb32600659"]/div
    # //*[@id="thumb32600659"]
    # //*[@id="list1"]/div/div/div/div[3]
    # //*[@id="list1"]/div/div/div
        html=driver.execute_script("return arguments[0].outerHTML;",fb)
        html_soup=b(html,'html.parser')
        print(html)
    
    # html_dur=driver.execute_script("return arguments[0].outerHTML;",fb_dur)
    # html_soup_dur=b(html_dur,'html.parser')
    
    # class_find=html_soup.findAll('div',{'class':'thumb-details'})
        class_find=html_soup.findAll('div',{'class':'thumb-content'})
        class_find_dur=html_soup.findAll('div',{'class':'poster'})
        print(class_find)
        print(class_find_dur)
    
   
        json_class_find = converter.convertAll(class_find)
        json_class_find_dur = converter.convertAll(class_find_dur)
        
        print(json_class_find)
        print(json_class_find_dur)
        
        
    # print(json_class_find[181]['div']['a']['span'][0]['span']['text'])
    # print(json_class_find[1]['p']['span']['text'])
    # print(json_class_find[1]['div']['span'][0]['text'])#viwe
    # print(json_class_find[1]['div']['span'][1]['text'])#publish_time
        meta_dic = {'content_name': '', 'viwes':'','channel_name':'','content_link':'','description':'','publish_time':'','duration':''}
        # engine = create_engine('postgresql://postgres:12344321@10.32.141.17/SazmanPro',pool_size=20, max_overflow=100,)
        # con=engine.connect()
    # json_class_find111111 = json_class_find
    # json_class_find111111 = pd.DataFrame()
    # json_class_find111111.to_excel(r'D:\python\source code\aparat_crawler\row_data\aparat.xlsx', index=False)
    # i = 0
        for i in range(len(json_class_find)):
            print(i)
        # print(len(json_class_find))
        # print(json_class_find[i])
        # print(json_class_find[i]['a<span>استقلال</span>']['span']['text'])#content title
            meta_dic['content_name']=json_class_find[i]['div']['a']['span']['text']
            print(meta_dic['content_name'])#content title
        # print(json_class_find[i]['a']['attributes']['href'])#content link
            meta_dic['content_link']='https://www.aparat.com/'+json_class_find[i]['div']['a']['attributes']['href']
            print(meta_dic['content_link'])#content_link
            try:
                # meta_dic['channel_name']
                print(json_class_find[i]['div']['div']['a']['span']['span']['text'])#channel name
            # print(json_class_find[i] ['div']['a']['span']['text'])
            # print(json_class_find[i]['div']['div']['a']['span']['text'])#channel name
                meta_dic['channel_name']=json_class_find[i]['div']['div']['a']['span']['span']['text']
            except:
                try:
                    print(json_class_find[i]['div']['div']['a']['span']['span'][0]['text'])   
                    meta_dic['channel_name']=json_class_find[i]['div']['div']['a']['span']['span'][0]['text']
                except:
                    pass
        
            try:
                # print(json_class_find[i]['div']) ['p'])['span'] ) ['div'] ['span'] [0]  ['attributes'] ["class"] )
                # print(json_class_find[i]['div'] ['div'] ['span'] [0]  ['attributes'] ["class"] )            ['p']['span']['text'])# description
                meta_dic['description']=json_class_find[i]['div']['p']['span']['text']
            except:
                pass
            
            try:
                # print(json_class_find[i]['div']['div']['span'][0])
                print(json_class_find[i]['div']['div']['span'][0]['text'])#viwe
        # print(json_class_find[i]['div']['div'][1]['span'][0]['text'])#viwe
        
                meta_dic['viwes']=json_class_find[i]['div']['div']['span'][0]['text']
            except :
                pass
        # print(meta_dic['viwes'])
            # pattern = '[ا-ی]'
            # if 'هزار' in json_class_find[i]['div']['div'][1]['span'][0]['text']:
            #     meta_dic['viwes']=(float(re.sub(pattern,'', json_class_find[i]['div']['div'][1]['span'][0]['text']))*1000)*1000

            # if 'میلیون' in json_class_find[i]['div']['div'][1]['span'][0]['text']:
            #     meta_dic['viwes']=(float(re.sub(pattern,'', json_class_find[i]['div']['div'][1]['span'][0]['text']))*1000000)*1000000
            # else:
            #     meta_dic['viwes']=re.sub(pattern,'', json_class_find[i]['div']['div'][1]['span'][0]['text'])
        
            try:
                print(json_class_find[i]['div']['div']['span'][1]['text'])#publish_time
                meta_dic['publish_time']=json_class_find[i]['div']['div']['span'][1]['text']
            except:
                pass
        ##duration
            try:
                print(json_class_find_dur[i]['div']['a']['div'][1]['span']['text'])#duration
                meta_dic['duration']=json_class_find_dur[i]['div']['a']['div'][1]['span']['text']
            except:
                pass
        
        # print(meta_dic)
            date_i=datetime.datetime.now()
            meta_dic['crawling_date']=str(date_i.date()).replace('-','')+str(date_i.time()).split(':')[0]
            meta_dic['orginal_name']=input_content_name
            data_frame =pd.DataFrame(meta_dic,index=[0])
            # data_frame.to_sql('aparart_14010407_out_2',con,if_exists='append', index=False)
            
            # con.close()
            
            data_frame_1 = data_frame_1.append(data_frame)
            
        data_frame_1.to_excel(r'D:\python\source code\aparat_crawler\out_test\aparart_14010408_out_6.xlsx', index=False)

      #
        ii = ii+1 
    except:
        pass       
