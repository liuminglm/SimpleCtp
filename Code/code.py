# -*- coding: utf-8 -*-
"""
Created on Wed Aug 02 22:43:57 2017

@author: ming
"""

import numpy as np
import requests as re
import time
from datetime import datetime,timedelta 
import cPickle as pickle 
from  concurrent import futures
from Queue import Queue
import pandas as pd
from Queue import Queue
from WindPy import *
w.start()



def wind():
    con = True
    while con:
        date_new_shf = 'date='+str(datetime.now())[0:4]+str(datetime.now())[5:7]+str(datetime.now())[8:10]+';sectorId=a599010201000000'
        date_new_dce = 'date='+str(datetime.now())[0:4]+str(datetime.now())[5:7]+str(datetime.now())[8:10]+';sectorId=a599010301000000'
        date_new_czc = 'date='+str(datetime.now())[0:4]+str(datetime.now())[5:7]+str(datetime.now())[8:10]+';sectorId=a599010401000000'
        SHF = w.wset("SectorConstituent",date_new_shf)
        DCE = w.wset("SectorConstituent",date_new_dce)
        CZC = w.wset("SectorConstituent",date_new_czc)
        if SHF.ErrorCode == DCE.ErrorCode == CZC.ErrorCode == 0:
            con = False
        else:
            pass
    

    
    daima_all = []
    market = [DCE,SHF,CZC]
    for mr in market:
        #print mr
        for ss in mr.Data[1]:
            tmp = ''
            for sss in ss:
                if sss.isalpha():
                    tmp = tmp+sss
                else:
                    break
            if daima_all.count(tmp) == 0:
                daima_all.append(tmp)
            else:
                pass
            tmp = ''
    

    daima_dict = {}
    for sig in daima_all:
        #print sig
        same_daima = []
        for mr in market:
            for ss in mr.Data[1]:
                tmp = ''
                for sss in ss:
                    if sss.isalpha():
                        tmp = tmp+sss
                    else:
                        break
                if tmp == sig:
                    same_daima.append(ss)
                else:
                    pass
        daima_dict[sig] = same_daima
                    

    daima_position = {}
    for daima_check in daima_dict.keys():
        #print daima_check
        #print daima_dict[daima_check]
        position_check = 0.0
        
        for dc in daima_dict[daima_check]:
            data_check = w.wsd(dc, "oi", str(datetime.now()-timedelta(days = 20))[0:10], str(datetime.now())[0:10], "")
            if data_check.Data[0][-1]>position_check:
                position_check = data_check.Data[0][-1]
                if position_check>10000:
                    daima_position[daima_check.upper()] = dc
                else:
                    pass
            else:
                pass
            
    
        
    return daima_position
    


def hexun():
    q = Queue()

    daima_all = []

    code_list = ['430','431','432']

    for co in code_list:
        if co == '430':
            market = 'SHFE'
        elif co == '431':
            market = 'DCE'
        else:
            market = 'CZCE'
        for pages in ['0','100','200','300','400','500','600','700','800']:
            url = 'http://webftcn.hermes.hexun.com/shf/sortlist?block='+co+'&number=100&title=14&commodityid=0&direction=0&start='+pages+'&column=code,dateTime&callback=hx_json'
            con = True
            while con:
                r = re.get(url)
                if r.status_code == 200:
                    con = False
                else:
                    pass
            if len(r.content.split('Data":')[1])<10:
                pass
            else:
                #print r.content
                for sig in  r.content.split('Data":')[1][4:].split('],["'):
                    try:
                        int(sig.split(',')[0].strip('"')[-3:])
                        daima_all.append(sig.split(',')[0].strip('"')+'.'+market)
                    except:
                        pass
    '''============================================================================================================================================='''

    daima_category = {}
    pinzhong = []
    for sig in daima_all:
        tmp = ''
        for ss in sig.split('.')[0]:
            if ss.isalpha():
                tmp = tmp+ss
            else:
                pass
        
        if pinzhong.count(tmp) == 1:
            pass
        else:
            pinzhong.append(tmp)
        del tmp
        

    for pz in pinzhong:
        same_pz = []
        for sig in daima_all:
            tmp = ''
            for ss in sig.split('.')[0]:
                if ss.isalpha():
                    tmp = tmp+ss
                else:
                    pass
            if tmp == pz:
                same_pz.append(sig)
            else:
                pass
        daima_category[pz] = same_pz
    #print daima_category
    '''============================================================================================================================================='''


    def get_positon(daima):
        market = daima.split('.')[1]
        dm = daima.split('.')[0]
        #daima = CZCEMA1701
        url_1 = 'http://webftcn.hermes.hexun.com/shf/quotelist?code='
        url_2 = '&column=Code,DateTime,OpenInterest'
        po = []#position
        for m in range(4):
            if m == 0:
                daima_new = market+dm
            else:
                daima_new = market+str(m)+dm
        
            url = url_1+daima_new+url_2
            
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
            try:
                p = float(r.text.split(',')[-1].split(']')[0])
                po.append(p)
            except:
                po.append(0.0)
            
        return max(po)
    '''=================================================================================='''    
    def sure_daima(dm,daima_category):
        max_holder = {}
        #print dm
        p_max = 0
        for dm2 in daima_category[dm]:
            p = get_positon(dm2)
            if p <10000:
                pass
            else:
                if p>p_max:
                    max_holder[dm] = dm2
                    p_max = p
                else:
                    pass
        if len(max_holder.keys()) == 0:
            pass
        else:
            q.put(max_holder)
            
    with futures.ThreadPoolExecutor(max_workers=16) as executor:
        future_to_url = {executor.submit(sure_daima, dm,daima_category): dm for dm in daima_category.keys()}
    num = q.qsize()
    max_holder = {}
    for qq in range(num):
        data_get_q = q.get()
        max_holder[data_get_q.keys()[0]] = data_get_q.values()[0]      
    return max_holder


def code_set():
    code_hexun = hexun()
    code_wind = wind()
    
    print len(code_hexun)
    print len(code_wind)
    code = {}
    for dm in code_wind.keys():
        code[dm] = {'wind':code_wind[dm],'hexun':code_hexun[dm]}
    return code

print code_set()

    
    
    
    
