# -*- coding: utf-8 -*-
"""
Created on Thu Nov 02 10:28:14 2017

@author: ming
"""

import numpy as np
import requests as re
import time
from datetime import datetime,timedelta 
import cPickle as pickle 
from  concurrent import futures
from Queue import Queue



def trade_calendar():
    sj = str(datetime.now())[0:4]+str(datetime.now())[5:7]+str(datetime.now())[8:10]+'150000'
    url = 'http://webstock.quote.hermes.hexun.com/a/kline?code=sse000001&start='+sj+'&number=-1000&type=5&callback=callback'
    con = True
    while con:
        r = re.get(url)
        if r.status_code == 200:
            con = False
        else:
            pass
    
    d = r.text
    tmp = []
    for shijian in d.split('"Data":[[[')[1].split('],['):
        tmp.append(shijian.split(',')[0][0:8])
    sj_now = datetime(int(str(datetime.now())[0:4]),
                      int(str(datetime.now())[5:7]),
                        int(str(datetime.now())[8:10]),
                        0,
                        0,
                        0)
    if (datetime(int(tmp[-1][0:4]),int(tmp[-1][4:6]),int(tmp[-1][6:]),0,0,0)-sj_now).seconds ==0:
        if datetime.now()<datetime(int(str(datetime.now())[0:4]),int(str(datetime.now())[5:7]),int(str(datetime.now())[8:10]),15,30,0):
            del tmp[-1]
        else:
            pass
    else:
        pass
    return tmp[-30:]

def history_data(daima_list,date_input_list):
    data_save = {}
    for dm_keys in daima_list.keys():
        dm = daima_list[dm_keys]
        shijian_list = []
        closep = []
        volume = []
        amount = []
        position = []
            
        for date_input in date_input_list:
            market = dm.split('.')[1]
            url_1 = 'http://webftcn.hermes.hexun.com/shf/historyminute?code='
            code = market+dm.split('.')[0]
            shijian = '&date='+date_input#20160928
            url = url_1+code+shijian
            
            con = True
            while con:
                try:
                    r = re.get(url,timeout = 3)
                    if r.status_code == 200:
                        con = False
                    else:
                        print 'pass'
                        
                        print r.status_code
                        print url
                        pass
                except:
                    pass
         
            
            data = r.text.split('"Data":')[1].split(']],')[0][3:]
            for a in data.split('], ['):
                date_tmp = a.split(',')[0][0:4]+'-'+a.split(',')[0][4:6]+'-'+a.split(',')[0][6:8]
                shijian_tmp = a.split(',')[0][8:10]+':'+a.split(',')[0][10:12]+':'+a.split(',')[0][12:]
                #shijian.append(str(time.strptime(date_tmp+' '+shijian_tmp,'%Y-%m-%d %H:%M:%S')))
                shijian_list.append((date_tmp+' '+shijian_tmp).encode('utf-8'))
                closep.append(float(a.split(',')[1]))
                amount.append(float(a.split(',')[2]))
                volume.append(float(a.split(',')[3]))
                position.append(float(a.split(',')[5]))
        shijian_close = {'time':shijian_list,
                         'close':closep,
                         'amount':amount,
                         'volume':volume,
                         'position':position}

        data_save[dm_keys] = shijian_close
        del shijian_close
    return data_save




            
def url_mins(daima_list):
    def url_check(dm,daima_list):
        def url_get(url):
            con = True
            while con:
                try:
                    r1 = re.get(url)
                    if r1.status_code == 200:
                        con = False
                    else:
                        pass
                except:
                    pass
            return r1
            
        market = daima_list[dm].split('.')[1]
        daima = daima_list[dm].split('.')[0]#.values()[0]
        
        
        today = datetime.now()+timedelta(days = 1)
        date_str = '&start='+str(today)[0:4]+str(today)[5:7]+str(today)[8:10]+'150000'
        
        url_part1 = 'http://webftcn.hermes.hexun.com/shf/minute?code='
        url_part2 = '&number=-800&t='
    
    
    
        con_all = True
        url_set = {}
        while con_all:
            daima_new_1 = market+'3'+daima
            url_1 = url_part1+daima_new_1+date_str+url_part2
            r1 = url_get(url_1)
            url_set[url_1] = r1
            
            
            daima_new_2 = market+daima
            url_2 = url_part1+daima_new_2+date_str+url_part2
            r2 = url_get(url_2)
            url_set[url_2] = r2
            
            
            
            daima_new_3 = market+'2'+daima
            url_3 = url_part1+daima_new_3+date_str+url_part2
            r3 = url_get(url_3)
            url_set[url_3] = r3
    
            daima_new_4 = market+'4'+daima
            url_4 = url_part1+daima_new_4+date_str+url_part2
            r4 = url_get(url_4)
            url_set[url_4] = r4
    
            daima_new_0 = market+'1'+daima
            url_0 = url_part1+daima_new_0+date_str+url_part2
            r0 = url_get(url_0)
            url_set[url_0] = r0
    
            url_tmp = ''
            for r_sig in url_set.keys():
                if len(url_set[r_sig].text.split('Data":[[')[1].split('],['))>2:
                    con_all = False
                    url_tmp = r_sig
                else:
                    pass       
        return url_tmp
    
    daima_url = {}
    for dm in daima_list.keys():
        daima = daima_list[dm].split('.')[0]
        daima_url[dm] = url_check(dm,daima_list)
    return daima_url
    



            
def url_day(daima_list):
    def url_check(dm,daima_list):
        def url_get(url):
            con = True
            while con:
                try:
                    r1 = re.get(url)
                    if r1.status_code == 200:
                        con = False
                    else:
                        pass
                except:
                    pass
            return r1
            
        market = daima_list[dm].split('.')[1]
        daima = daima_list[dm].split('.')[0]#.values()[0]
        
        
        today = datetime.now()+timedelta(days = 1)
        date_str = '&start='+str(today)[0:4]+str(today)[5:7]+str(today)[8:10]+'210000'
        
        url_part1 = 'http://webftcn.hermes.hexun.com/shf/kline?code='
        url_part2 = '&number=-1000&type=5'
    
    
    
        con_all = True
        url_set = {}
        while con_all:
            daima_new_1 = market+'3'+daima
            url_1 = url_part1+daima_new_1+date_str+url_part2
            r1 = url_get(url_1)
            url_set[url_1] = r1
            
            
            daima_new_2 = market+daima
            url_2 = url_part1+daima_new_2+date_str+url_part2
            r2 = url_get(url_2)
            url_set[url_2] = r2
            
            
            
            daima_new_3 = market+'2'+daima
            url_3 = url_part1+daima_new_3+date_str+url_part2
            r3 = url_get(url_3)
            url_set[url_3] = r3
    
            daima_new_4 = market+'4'+daima
            url_4 = url_part1+daima_new_4+date_str+url_part2
            r4 = url_get(url_4)
            url_set[url_4] = r4
    
            daima_new_0 = market+'1'+daima
            url_0 = url_part1+daima_new_0+date_str+url_part2
            r0 = url_get(url_0)
            url_set[url_0] = r0
    
            url_tmp = ''
            for r_sig in url_set.keys():#[r0,r1,r2,r3,r4]:
                #print url_set[r_sig].text.split('Data":[[')[1].split('],[')
                if len(url_set[r_sig].text.split('Data":[[')[1].split('],['))>2:
                    con_all = False
                    url_tmp = r_sig
                else:
                    pass
                
        return url_tmp
    daima_url = {}
    for dm in daima_list.keys():
        daima = daima_list[dm].split('.')[0]
        daima_url[dm] = url_check(dm,daima_list)
    return daima_url
    
    



data_all = {}
date_list = trade_calendar()   
daima_set = {}
with open('E:\SimpleCtp\Code\data.pickle', "rb") as input_file:
    daima_all = pickle.load(input_file)
    for dm in daima_all.keys() :
        daima_set[dm] = daima_all[dm]['hexun'] 
    data_all['daima_all'] = daima_set
data_all['data_history'] = history_data(data_all['daima_all'],date_list)   
data_all['url'] = url_mins(data_all['daima_all'])
data_all['url_day'] = url_day(data_all['daima_all'])
with open('data.pickle', "wb") as input_file:
    pickle.dump(data_all, input_file)

