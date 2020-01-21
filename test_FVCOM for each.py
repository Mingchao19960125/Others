#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 14:00:47 2019

@author: Mingchao
"""
import pandas as pd
import json
import os
import datetime
import netCDF4
import numpy as np
import codecs
import math
###Hardcodes##
path='/home/jmanning/Mingchao/parameter/dictionary.json'
path_save='/home/jmanning/Mingchao/others/FVCOM/'
#FVCOM_csv=pd.read_csv('/home/jmanning/Mingchao/others/FVCOM.csv')
#vessel_name='Charger'
Dictionary_save='/home/jmanning/Mingchao/parameter/each_vessels/'
### main ####
with open(path,'r') as fp:
    dict=json.load(fp)
for name in dict.keys(): #
    #if name==vessel_name:
    if name!='end_time':
        df=pd.DataFrame.from_dict(dict[name])
        df['time']=df.index
        FVCOM_df=df[['time','lat','lon','FVCOM_T','FVCOM_H']]
        FVCOM_df.rename(columns={'FVCOM_T':'temp','FVCOM_H':'depth'},inplace=True)
        depth_dict=dict[name]['observation_H']
        depth_df=pd.DataFrame(list(depth_dict.items()),columns=['time','depth'])
        depth_df.index=depth_df['time']
        FVCOM_df['depth']=depth_df['depth']
        if not os.path.exists(path_save):
            os.makedirs(path_save)
    #FVCOM_df.to_csv(os.path.join(path_save,'FVCOM.csv'),index=0)
    FVCOM_df.to_csv(os.path.join(path_save,name+'_FVCOM.csv'),index=0)
    FVCOM_list=[]
    try:
        #for i in FVCOM_csv.index:
        FVCOM_csv=pd.read_csv(os.path.join(path_save,name+'_FVCOM.csv'))
        for i in FVCOM_csv.index:
            lati=FVCOM_csv['lat'][i]
            loni=FVCOM_csv['lon'][i]            
            depthi=FVCOM_csv['depth'][i]
            dtime=datetime.datetime.strptime(FVCOM_csv['time'][i],'%Y-%m-%d %H:%M:%S')
            #dtime=datetime.datetime.strptime(FVCOM_csv[i],'%Y-%m-%d %H:%M:%S')
            FVCOM=get_FVCOM_temp(latp=lati,lonp=loni,dtime=dtime,depth=depthi,mindistance=2,fortype='tempdepth')
            FVCOM_list.append(FVCOM[0].filled())
            #for key in dict[vessel_name]['FVCOM_T']:
            for key in dict[name]['FVCOM_T']:
                if key == FVCOM_csv['time'][i]:
                #if key == FVCOM_csv[i]:
                #if key == datetime.datetime.strptime(FVCOM_csv[i],'%Y-%m-%d %H:%M:%S'):
                    #dict[vessel_name]['FVCOM_T'][key]=float(FVCOM_list[i])
                    dict[name]['FVCOM_T'][key]=float(FVCOM_list[i])
            for key in dict[name]['FVCOM_H']:
                if key == FVCOM_csv['time'][i]:
                    dict[name]['FVCOM_H'][key]=float(FVCOM_csv['depth'][i])
                else:
                    continue
    except:
        print(FVCOM_csv['time'][i])
        #print(FVCOM_csv[i])
        #with codecs.open(Dictionary_save+vessel_name+'_dictionary.json','a','utf-8') as outf:
    with codecs.open(Dictionary_save+name+'_dictionary.json','a','utf-8') as outf:
        json.dump(dict,outf,ensure_ascii=False)
    
    

def get_FVCOM_url(dtime):
    """dtime: the formate of time is datetime"""
    if (dtime-datetime.datetime.now())>datetime.timedelta(days=-2):
        url='http://www.smast.umassd.edu:8080/thredds/dodsC/FVCOM/NECOFS/Forecasts/NECOFS_GOM3_FORECAST.nc' 
    #elif dtime>=datetime.datetime(2019,1,1):
    elif dtime>=datetime.datetime(2016,7,1):
        #if dtime.month!=datetime.datetime.now().month:
        if dtime.month!=6:
            url='http://www.smast.umassd.edu:8080/thredds/dodsC/models/fvcom/NECOFS/Archive/NECOFS_GOM/2019/gom4_201907.nc'
            url=url.replace('201907',dtime.strftime('%Y%m'))
            url=url.replace('2019',dtime.strftime('%Y'))
        else:
            url=np.nan      
    else:
        url=np.nan
    return url

def get_FVCOM_temp(latp,lonp,dtime,depth='bottom',mindistance=2,fortype='temperature'): # gets modeled temp using GOM3 forecast 
    ''' 
    fortype list ['tempdepth','temperature']
    the unite of the mindistance is mile
    Taken primarily from Rich's blog at: http://rsignell-usgs.github.io/blog/blog/2014/01/08/fvcom/ on July 30, 2018 
    where lati and loni are the position of interest, dtime is the datetime, and layer is "-1" for bottom 
    '''
    m2k_factor = 0.62137119 #mile to kilometers parameter
    #urlfvcom = 'http://www.smast.umassd.edu:8080/thredds/dodsC/fvcom/hindcasts/30yr_gom3' 
    #urlfvcom = 'http://www.smast.umassd.edu:8080/thredds/dodsC/FVCOM/NECOFS/Forecasts/NECOFS_FVCOM_OCEAN_MASSBAY_FORECAST.nc' 
#    urlfvcom = 'http://www.smast.umassd.edu:8080/thredds/dodsC/FVCOM/NECOFS/Forecasts/NECOFS_GOM3_FORECAST.nc' 
    urlfvcom=get_FVCOM_url(dtime)
    #if math.isnan(urlfvcom):
    if urlfvcom==np.nan:
        if fortype=='temperature':
            return np.nan
        elif fortype=='tempdepth':
            return np.nan,np.nan
        else:
            'please input write fortype'
    nc = netCDF4.Dataset(urlfvcom).variables 
    #first find the index of the grid  
    lat = nc['lat'][:] 
    lon = nc['lon'][:] 
    inode,dist= nearlonlat(lon,lat,lonp,latp) 
    if dist>mindistance/m2k_factor/111:
        return np.nan,np.nan
    #second find the index of time 
    time_var = nc['time']
    itime = netCDF4.date2index(dtime,time_var,select='nearest')# where startime in datetime
    if depth=='bottom':
        layer=-1
    else:
        depth_distance=abs(nc['siglay'][:,inode]*nc['h'][inode]+depth)
        layer=np.argmin(depth_distance)
    
    if fortype=='temperature':
        return nc['temp'][itime,layer,inode]
    elif fortype=='tempdepth':    
        return nc['temp'][itime,layer,inode],nc['h'][inode]
    else:
        return 'please input write fortype'


def nearlonlat(lon,lat,lonp,latp): # needed for the next function get_FVCOM_bottom_temp 
    """ 
    i,min_dist=nearlonlat(lon,lat,lonp,latp) change 
    find the closest node in the array (lon,lat) to a point (lonp,latp) 
    input: 
        lon,lat - np.arrays of the grid nodes, spherical coordinates, degrees 
        lonp,latp - point on a sphere 
    output: 
        i - index of the closest node 
        min_dist - the distance to the closest node, degrees 
        For coordinates on a plane use function nearxy 
        Vitalii Sheremet, FATE Project   
    """ 
    cp=np.cos(latp*np.pi/180.) 
    # approximation for small distance 
    dx=(lon-lonp)*cp 
    dy=lat-latp 
    dist2=dx*dx+dy*dy 
    # dist1=np.abs(dx)+np.abs(dy) 
    i=np.argmin(dist2) 
    min_dist=np.sqrt(dist2[i]) 
    return i,min_dist
 
 