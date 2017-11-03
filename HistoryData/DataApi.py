# -*- coding: utf-8 -*-
"""
Created on Thu Nov 02 21:08:06 2017

@author: ming
"""


import numpy as np
import requests as re
import time
from datetime import datetime,timedelta 
import cPickle as pickle 
from  concurrent import futures
from Queue import Queue


def instant(daima):
    with open('data.pickle', "rb") as input_file:
        data_full = pickle.load(input_file)
    daima_url = data_full['url']
    url = daima_url[daima]
    con = True
    while con:
        try:
            r = re.get(url)
            if r.status_code == 200:
                con = False
            else:
                pass
        except:
            pass

    data = r.text.split('"Data":')[1].split(']],')[0][3:]
    closep = [float(a.split(',')[1]) for a in data.split('],[')]
    shijian = [str(a.split(',')[0])[0:4]+'-'+str(a.split(',')[0])[4:6]+'-'+str(a.split(',')[0])[6:8]+' '+str(a.split(',')[0])[8:10]+':'+str(a.split(',')[0])[10:12]+':'+str(a.split(',')[0])[12:14] for a in data.split('],[')]
    df = {'close':closep,
          'time':shijian}
    return df

def instant_day(daima):
    with open('data.pickle', "rb") as input_file:
        data_full = pickle.load(input_file)
    daima_url = data_full['url_day']
    url = daima_url[daima]
    con = True
    while con:
        try:
            r = re.get(url)
            if r.status_code == 200:
                con = False
            else:
                pass
        except:
            pass


    data = r.text.split('"Data":')[1].split(']],')[0][3:]
    shijian = [str(a.split(',')[0])[0:4]+'-'+str(a.split(',')[0])[4:6]+'-'+str(a.split(',')[0])[6:8]+' '+str(a.split(',')[0])[8:10]+':'+str(a.split(',')[0])[10:12]+':'+str(a.split(',')[0])[12:14] for a in data.split('],[')]
    closep = [float(a.split(',')[1]) for a in data.split('],[')]
    openp = [float(a.split(',')[2]) for a in data.split('],[')]
    high = [float(a.split(',')[4]) for a in data.split('],[')]
    low = [float(a.split(',')[5]) for a in data.split('],[')]
    volume = [float(a.split(',')[6]) for a in data.split('],[')]
    amount = [float(a.split(',')[7]) for a in data.split('],[')]
    
    df = {'close':closep,
          'open':openp,
          'high':high,
          'low':low,
          'volume':volume,
          'amount':amount,
          'time':shijian}
    return df
    
def data_combine(data_his,data_ins):
    #data_new = data_his+data_ins
    df = {'close':data_his['close']+data_ins['close'],
          'time':data_his['time']+data_ins['time']}
    return df
    
    
def convertion(df,mins):
    date_list = []
    '''检查时间'''
    sj_tmp_check= []
    for stc in df['time']:
        if len(stc) == 19:
            sj_tmp_check.append(stc)
        else:
            sj_tmp_check.append(stc[0:19])
    df['time'] = sj_tmp_check
    for d in df['time']:
        date_list.append(str(datetime.strptime(d, "%Y-%m-%d %H:%M:%S").date()))
    df['riqi'] = date_list
    str_to_datetime = [datetime.strptime(std, "%Y-%m-%d %H:%M:%S") for std in df['time']]
    df['time'] = str_to_datetime
    

    date_diff = []
    for d_1 in date_list:
        if date_diff.count(d_1) == 1:
            pass
        else:
            date_diff.append(d_1)
      
    closep_origin = df['close']
    shijian = df['time']

    closep = []
    strtime = []
    for date_today in date_diff:

        st = datetime(int(date_today[0:4]),int(date_today[5:7]),int(date_today[8:]),9,0,0)
        next_day = str(datetime(int(date_today[0:4]),int(date_today[5:7]),int(date_today[8:]))+timedelta(days = 1))
        end = datetime(int(next_day[0:4]),int(next_day[5:7]),int(next_day[8:10]),5,0,0)
        
        con = True
        save = []
        while con:
            if len(save) == 0:
                save.append([st,st+timedelta(seconds = 60*mins)])
                st_tmp = st
                end_tmp = st_tmp+timedelta(seconds = 60*mins)
            else:
                st_tmp = save[-1][1]
                end_tmp = st_tmp+timedelta(seconds = 60*mins)
                save.append([st_tmp,end_tmp])
                
            if end_tmp <= end:
                closep_tmp = []
                strtime_tmp = []
                n = 0
                for js in shijian:
                    if st_tmp<=js and js<end_tmp:
                        closep_tmp.append(float(closep_origin[n]))
                        strtime_tmp.append(shijian[n])
                        n = n+1
                    else:
                        n = n+1
                if len(closep_tmp) ==0:
                    pass
                else:
                    closep.append(float(closep_tmp[-1]))
                    strtime.append(strtime_tmp[0])
            else:
                con = False
            
    ddff = {'time':strtime,
            'close':closep}
    return ddff    


def data_mins_full(daima,mins):
    with open('data.pickle', "rb") as input_file:
        data_full = pickle.load(input_file)
    data_his = data_full['data_history'][daima]
    data_ins = instant(daima)
    return convertion(data_combine(data_his,data_ins),mins)
    
    
#print data_mins_full('V',5)['close'][-10:]

