# -*- coding: utf-8 -*-
"""
Created on Fri Jan 21 14:59:10 2022

@author: PC
"""
print('به نام خدا')


import pandas as pd
import numpy as np
import re
import time
import pyodbc 
from sqlalchemy import create_engine
from pyodbc import *
import time
import datetime
import psycopg2
import pandas.io.sql as psql


import datetime
from sqlalchemy import create_engine
import re
# from num2fawords import words, ordinal_words
from parsivar import Normalizer


total_dataframe = pd.DataFrame()
aparat_new_time = pd.DataFrame()
# import time
# import datetime

# start = time.time() 
# ###########################
# connection = psycopg2.connect(user="postgres",
#                             password="12344321",
#                             host="10.32.141.17",
#                             port="5432",
#                             database="SazmanPro")
# cursor = connection.cursor()

# input_ch1= psql.read_sql('select * from public."FinalChOne_01"', connection)
# print(len(input_ch1))

# input_ch2= psql.read_sql('select * from public."FinalChTwo_01"', connection)
# print(len(input_ch2))

# input_ch3= psql.read_sql('select * from public."FinalChThree_01"', connection)
# print(len(input_ch3))

# input_ch4= psql.read_sql('select * from public."FinalChFour_01"', connection)
# print(len(input_ch4))

# input_ch5= psql.read_sql('select * from public."FinalChFive_01"', connection)
# print(len(input_ch5))

# input_ch5= psql.read_sql('select * from public."FinalChFive_01"', connection)
# print(len(input_ch5))

# input_ch5= psql.read_sql('select * from public."FinalChFive_01"', connection)
# print(len(input_ch5))



total_dataframe = pd.read_excel(r'D:\python\source code\aparat_crawler\out_test\total aparat.xlsx')
#

# print(input0)
# input_ch1()
# input_ch1['شبکه'] = 'شبکه 1'
# input_ch2['شبکه'] = 'شبکه 2'
# input_ch3['شبکه'] = 'شبکه 3'
# input_ch4['شبکه'] = 'شبکه 4'
# input_ch5['شبکه'] = 'شبکه 5'

# # del total_dataframe
# total_dataframe = pd.DataFrame()
# total_dataframe = total_dataframe.append(input_ch1)
# total_dataframe = total_dataframe.append(input_ch2)
# total_dataframe = total_dataframe.append(input_ch3)
# total_dataframe = total_dataframe.append(input_ch4)
# total_dataframe = total_dataframe.append(input_ch5)


print(len(total_dataframe))

  
# total_dataframe.to_excel(r"D:\python\source code\aparat_crawler\out_test\total_dataframe_aparat.xlsx", index=False)

# total_dataframe.loc[(total_dataframe['c_views'].isnull()),'c_views'] = "سایر"
total_dataframe.loc[(total_dataframe['viwes'].isnull()),'viwes'] = "سایر"

# total_dataframe_11 = total_dataframe.query(" content_name != 'با سرویس تبلیغات آپارات، راحت ‌‌‌‌‌تر و بیشتر دیده شوید!'")
# total_dataframe_12 = total_dataframe_11.query(" content_name != '%50 تخفیف زمستونی آپارات کودک برای ورود به دنیای شاد کارتونی'")
# total_dataframe_13 = total_dataframe_12.query(" content_name != 'اطلاعات بیشتر'")
# total_dataframe_14 = total_dataframe_13.query(" content_name != 'تماشای انیمیشن آواز 2 با بهترین کیفیت و دوبله فارسی در آپارات کودک!'")
# total_dataframe_15 = total_dataframe_14.query(" content_name != 'تماشای جدیدترین انیمیشن ها در دنیای شاد کارتونی آپارات کودک با 50% تخفیف ویژه'")
aparat0 = total_dataframe.query(" viwes != 'سایر'")
aparat = aparat0[~aparat0['viwes'].str.contains('پیش')]
# aparat = aparat0.query(" viwes != 'پیش'")

# total_dataframe.loc[(total_dataframe['viwes'].isnull()),'viwes'] = "سایر"

    
# total_dataframe_2001.to_excel(r"D:\python\source code\aparat_crawler\out_test\modify_aparat00.xlsx", index=False)

print(len(aparat))

aparat[['count', 'scale', 'vahed']] = aparat['viwes'].str.split(' ', expand=True)
# aparat.head()

# aparat_scale = aparat[1:100]

# aparat_scale.to_excel(r"D:\python\source code\aparat_crawler\out_test\total_dataframe_aparat_scale.xlsx", index=False)

aparat['scale'] = aparat['scale'].str.replace('','1')


# 1ه1ز1ا1ر1



aparat['scale'] = aparat['scale'].str.replace('هزار','1000')
aparat['scale'] = aparat['scale'].str.replace('1ه1ز1ا1ر1','1000')
# aparat['scale'] = aparat['scale'].str.replace('','1')
aparat['scale'] = aparat['scale'].str.replace('میلیون','1000000')
aparat['scale'] = aparat['scale'].str.replace('1م1ی1ل1ی1و1ن1','1000000')

aparat['scale'] = aparat['scale'].str.replace('میلیارد','1000000000')
# aparat['scale'] = aparat['scale'].astype(str).astype(int)1
aparat.loc[(aparat['scale'].isnull()),'scale'] = "1"

aparat['scale'] = aparat['scale'].astype(str).astype(float)
aparat['count'] = aparat['count'].astype(str).astype(float)
aparat['value_view'] = ""

aparat['value_view'] = aparat['scale'] * aparat['count']


aparat[['day', 'time', 'fix']] = aparat['publish_time'].str.split(' ', expand=True)
aparat.head()

aparat['time'] = aparat['time'].str.replace('روز','ماه')
aparat['time'] = aparat['time'].str.replace('ساعت','ماه')
aparat['time'] = aparat['time'].str.replace('هفته','ماه')

del aparat['fix']
del aparat['vahed']
del aparat['scale']
# del aparat['vahed']
print(len(aparat))

import re

def splittime(x):
    
    a = re.split(":", x)
    if len(a)== 2:
        sum = int(a[0])+int(a[1])/60
    elif len(a) == 3:
        sum = int(a[0])*60+int(a[1])+int(a[2])/60
    elif len(a)==1:
        sum=int(a[0])/60
    else:
        sum=0
    return sum


aparat['new_time']=aparat['duration'].apply(lambda x:splittime(x))    


aparat_save = aparat.copy()
aparat = aparat_save.copy()

aparat.to_excel(r'D:\python\source code\aparat_crawler\out_test\aparat_save_test.xlsx', index=False)    
 
############ check with ehsan

# aparat['test'] = aparat.apply(lambda x: x['orginal_name'] in x['content_name'], axis=1)

# aparat['test'] = aparat['test'].astype(str)
# aparat_False = aparat.query("test == False")
# aparat_True = aparat.query("test == 'True'")


my_normalizer = Normalizer()

aparat['orginal_name'] = aparat['orginal_name'].apply(lambda x: my_normalizer.normalize(x))
a = set(aparat['orginal_name'].tolist())
my_normalizer = Normalizer()

def CNW(x):
    try:
        fcnv = re.findall('\d+', x)[0]
        icnv = ' ' + fcnv
        # ccnv = words(icnv)
        tx = re.sub(fcnv, icnv, x)
        text = my_normalizer.normalize(tx)
    except:
        text = my_normalizer.normalize(x)
        text = x
    return text

aparat['content_name'] = aparat['content_name'].apply(lambda x: CNW(x))


final_list = list()
final_df = pd.DataFrame()
for item in a:
    print(item)
    original_name = str(item)
    OnList = re.split(' ', original_name)
    if len(OnList) < 2:
        pattern = '(مجموعه تلویزیونی|سریال|برنامه|مجموعه|فیلم|کارتون|انیمیشن|فیلم سینمایی|مستند) {}'.format(
            original_name)
    else:
        pattern = '{}'.format(original_name)

    tmp_df = aparat[aparat['orginal_name'] == original_name]

    tmp = list(map(lambda x: re.search(pattern, x), tmp_df['content_name']))
    tmp_df['clean'] = tmp
    
    final_df = final_df.append(tmp_df)

print(len(aparat))
print(len(final_df))
    
# final_df.to_excel(r'D:\python\source code\aparat_crawler\out_test\final_df.xlsx', index=False)    
 
def tf(x):
    if x != None:
        return 'True'
    else:
        return 'False'

final_df['clean'] = final_df['clean'].apply(lambda x: tf(x))

final_df['description'] = final_df['description'].astype(str)

# c=final_df[1:1000]


final_df_1 = final_df[~final_df['content_name'].str.contains('خرید')]    
final_df_2 = final_df_1[~final_df_1['description'].str.contains('خرید')]
final_df_3 = final_df_2[~final_df_2['description'].str.contains('فروش')]
final_df_4 = final_df_3[~final_df_3['content_name'].str.contains('فروش')]  
print(len(final_df_4))


final_df_4.to_excel(r'D:\python\source code\aparat_crawler\out_test\total_clean29.xlsx', index=False)
# final_df_5 = final_df[(~final_df['description'].str.contains('خرید')) & (final_df['description'].str.contains('شبکه'))]
def check2(x):
    result = re.search('خرید|فروش', x)
    if result:
        result2= re.search('شبکه',x)
        if result2:
            return 'True'
        else:
            return 'False'
    else:
        return 'True'
    
df2 = final_df[final_df['clean'] != 'False']

df2['clean_1'] = df2['description'].apply(lambda x:check2(x))

df2.dtypes


df60 = final_df[final_df['content_name'].str.contains('شبکه', regex=False)]    

df60.to_excel(r'D:\python\source code\aparat_crawler\out_test\total_19.xlsx', index=False)
print(len(df60))

aparat_final = final_df_4.append(df60)

aparat_final.to_excel(r'D:\python\source code\aparat_crawler\out_test\total_clean_20.xlsx', index=False)

print(len(aparat_final))


aparat_final.dtypes

# aparat_new_time = aparat.query("test == 'True'")

aparat_new_time = aparat_final[aparat_final['new_time'] > 2]

# aparat_new_time = aparat_final['new_time'] > 2.0
# new_time

print(len(aparat_new_time))

del aparat_new_time['viwes']
del aparat_new_time['count']
aparat_new_time = aparat_new_time.rename(columns={'value_view': 'viwes'})

aparat_new_time.to_excel(r'D:\python\source code\aparat_crawler\out_test\total_clean_time_2.xlsx', index=False)
