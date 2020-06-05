#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 23 14:32:42 2019

Feb 28,2020 Mingchao
    input local time,then change the time style in functions of raw_tele_modules for filtering the data
APR 29,2020 Mingchao
    add TimeoutError, in case upload data to student drifter have issue
May 6,2020 Mingchao
    change the function of up.sd2drf for being suitable with Windows
@author: leizhao
"""

import raw_tele_modules as rdm
from datetime import datetime,timedelta
import os
import upload_modules as up
import ftpdownload


def week_start_end(dtime,interval=0):
    '''input a time, 
    if the interval is 0, return this week monday 0:00:00 and next week monday 0:00:00
    if the interval is 1,return  last week monday 0:00:00 and this week monday 0:00:00'''
    delta=dtime-datetime(2003,1,1,0,0)-timedelta(weeks=interval)
    count=int(delta/timedelta(weeks=1))
    start_time=datetime(2003,1,1,0,0)+timedelta(weeks=count)
    end_time=datetime(2003,1,1,0,0)+timedelta(weeks=count+1)   
    return start_time,end_time 
def main():
    realpath=os.path.dirname(os.path.abspath(__file__))
    #realpath='E:/programe/raw_data_match/py'
    parameterpath=realpath.replace('py','parameter')
    #HARDCODING
    raw_data_name_file=os.path.join(parameterpath,'raw_data_name.txt')  #this data conclude the VP_NUM HULL_NUM VESSEL_NAME
    #raw_data_name_file='E:/programe/raw_data_match/parameter/raw_data_name.txt'
    output_path=realpath.replace('py','result')  #use to save the data 
    telemetry_status=os.path.join(parameterpath,'telemetry_status.csv')
    #telemetry_status='E:/programe/aqmain/parameter/telemetry_status.csv'
    lack_data_path=os.path.join(output_path, 'lack_data.txt')
    #lack_data_path='E:/programe/raw_data_match/result/lack_data.txt'#store the name of file that lacked data after 'classfy finished'
    # below hardcodes is the informations to upload local data to student drifter. 
    subdir=['Matdata','checked']
    mremote='/Raw_Data'
    #mremote='\Raw_Data'
    remote_subdir=['Matdata','checked']
    ###########################
    end_time=datetime.utcnow()
    #start_time,end_time=week_start_end(end_time,interval=1)
    start_time=end_time-timedelta(weeks=1)
    #download raw data from website
    files=ftpdownload.download(localpath='E:\\programe\\raw_data_match\\result\\Matdata', ftppath='/Matdata')
    #classify the file by every boats
    rdm.classify_by_boat(indir='E:\\programe\\raw_data_match\\result\\Matdata',outdir='E:\\programe\\raw_data_match\\result\\classified',pstatus=telemetry_status)
    print('classfy finished!')
    #check the reformat of every file:include header,heading,lat,lon,depth,temperature.
    rdm.check_reformat_data(indir='E:\\programe\\raw_data_match\\result\\classified',outdir='E:\\programe\\raw_data_match\\result\\checked',startt=start_time,\
                        endt=end_time,pstatus=telemetry_status,rdnf=raw_data_name_file,lack_data=lack_data_path)
    print('check format finished!')
    for i in range(len(subdir)):
        local_dir=os.path.join(output_path,subdir[i])
        #remote_dir=os.path.join(mremote,remote_subdir[i])
        remote_dir=os.path.join(mremote,remote_subdir[i]).replace('\\', '/')
        #up.sd2drf(local_dir,remote_dir,filetype='csv',keepfolder=True)
        try:
            up.sd2drf(local_dir, remote_dir, filetype='csv', keepfolder=True)
        except TimeoutError:
            print('Timeout Error')
if __name__=='__main__':
    main()