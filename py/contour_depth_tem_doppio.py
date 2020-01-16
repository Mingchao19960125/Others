# -*- coding: utf-8 -*-
"""
Created on Mon Nov 12 11:27:15 2018

Draw contours and isothermal layers on the map
feb 11:Geographic box with full map in “insert” use “addlon=0.3”
Allow users hardcode at top of code to defive depth_contours_to_plot=[20, 50,100]
Allow users option of posting model node points

@author: leizhao

"""
import os
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(os.path.join(conda_dir, 'share'), 'proj')
os.environ["PROJ_LIB"] = proj_lib
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import netCDF4
import datetime
import zlconversions as zl
import doppio_modules as dm
import numpy as np
#HARDCODES
#input_date_time='2018-11-11 12:00:00'
input_date_time='2019-12-17 09:48:00'
#input_lat=41.784712
input_lat=42.73496
#input_lon=-69.231081
input_lon=-70.2993
input_depth=0   #If you enter 99999, the default output bottom temperature,enter 0 will output the temperature of surface
#input_depth=77.0
#output_path='/home/jmanning/Desktop/testout/doppio/'
output_path='/home/jmanning/Mingchao/programe/DOPPIO/'


addlon=0.3 # edges around point to include in the zoomed in plot
addlat=0.3#
mod_points='yes' # do you want to post model grid nodes
depth_contours_to_plot=[20, 50,100,150,200,500]
#########################
date_time=datetime.datetime.strptime(input_date_time,'%Y-%m-%d %H:%M:%S') # transform time format
#find index of the nearest time about data
for m in range(0,7):
    try:
        url_time=(date_time-datetime.timedelta(days=m)).strftime('%Y-%m-%d')#
        url=zl.get_doppio_url(url_time)
        #get the data 
        nc=netCDF4.Dataset(url)
    except:
        continue
    lons=nc.variables['lon_rho'][:]
    lats=nc.variables['lat_rho'][:]
    temp=nc.variables['temp']
    doppio_time=nc.variables['time']
    doppio_depth=nc.variables['h'][:]
    min_diff_time=abs(datetime.datetime(2017,11,1,0,0,0)+datetime.timedelta(hours=int(doppio_time[0]))-date_time) #Set initial value
    min_diff_index=0  #Set initial value
    for i in range(0,157):
        diff_time=abs(datetime.datetime(2017,11,1,0,0,0)+datetime.timedelta(hours=int(doppio_time[i]))-date_time)
        if diff_time<min_diff_time:
            min_diff_time=diff_time
            min_diff_index=i
    if min_diff_time<datetime.timedelta(hours=1):  #if min_diff_time less 1 hour, out of the loop
        break

#find the temperature and index of point
try:
    #point_temp,index_1,index_2=dm.get_doppio(lat=input_lat,lon=input_lon,depth=input_depth,time=input_date_time)
    #change fortype ='temp&depth' to get point_temp,index_1 and index_2 from doppio_modules.py
    point_temp,index_1,index_2=dm.get_doppio(lat=input_lat,lon=input_lon,depth=input_depth,time=input_date_time,fortype ='temp&depth')
except:
    print(input_date_time)
if input_depth==99999:   #if input_depth=99999, we need output the bottom layer
    S_coordinate=1   #1 represent the bottom layer, 0 represent the surface layer
else:
    S_coordinate=float(input_depth)/float(doppio_depth[index_1][index_2])

#creat map
#Create a blank canvas       
fig=plt.figure(figsize = (20, 20))
fig.suptitle('DOPPIO model bottom temp(deg C) and depth(meter)',fontsize=35, fontweight='bold')

#Draw contour lines and temperature maps in detail
ax1=fig.add_axes([0.07,0.03,0.85,0.95])
ax1.set_title(input_date_time, loc='center')
ax1.axes.title.set_size(24)

service = 'Ocean_Basemap'
map=Basemap(llcrnrlat=input_lat-addlat,urcrnrlat=input_lat+addlat,llcrnrlon=input_lon-addlon,urcrnrlon=input_lon+addlon,\
            resolution='f',projection='tmerc',lat_0=input_lat,lon_0=input_lon,epsg = 4269)
map.arcgisimage(service=service, xpixels = 5000, verbose= False)

#label the latitude and longitude
parallels = np.arange(0.,90,0.1)
map.drawparallels(parallels,labels=[1,0,0,0],fontsize=20,linewidth=0.0)
# draw meridians
meridians = np.arange(180.,360.,0.1)
map.drawmeridians(meridians,labels=[0,0,0,1],fontsize=20,linewidth=0.0)
lon,lat=map(lons,lats)
dept_clevs=depth_contours_to_plot
dept_cs=map.contour(lon,lat,doppio_depth,dept_clevs,colors='black')
plt.clabel(dept_cs, inline = True, fontsize =20,fmt="%1.0f")
if 0<=S_coordinate<1:
    temp_cs=map.contourf(lon,lat,temp[min_diff_index][39-int(S_coordinate/0.025)],7)#,cmap=cm.StepSeq)
elif S_coordinate==1:
    temp_cs=map.contourf(lon,lat,temp[min_diff_index][39],7)#,cmap=cm.StepSeq)
else:
    print ("the depth is out of the depth of bottom")
temp_cbar=map.colorbar(temp_cs,location='right',size="5%",pad="1%")
temp_cbar.set_label('deg C',size=25)
temp_cbar.ax.set_yticklabels(temp_cbar.ax.get_yticklabels(), fontsize=20)
if point_temp==9999:
    citys=['the depth is out of the bottom depth']
else:
    citys=[str(round(point_temp,2))]
lat_point=[input_lat]
lon_point=[input_lon]
x,y=map(lon_point,lat_point)
x1,y1=map(lons,lats)
plt.plot(x1,y1,'yo')
plt.plot(x,y,'ro')
plt.text(x[0]+0.02,y[0]-0.01,citys[0],bbox=dict(facecolor='yellow',alpha=0.5),fontsize =30)
#indert a map that have mmore screen 
ax2=fig.add_axes([0.09,0.68,0.2,0.2])
#Build a map background
map1=Basemap(llcrnrlat=int(input_lat)-5,urcrnrlat=int(input_lat)+5,llcrnrlon=int(input_lon)-5,urcrnrlon=int(input_lon)+5,\
            resolution='f',projection='tmerc',lat_0=int(input_lat),lon_0=int(input_lon),epsg = 4269)
map1.arcgisimage(service=service, xpixels = 5000, verbose= False)


parallels = np.arange(0.,90.,3)
map1.drawparallels(parallels,labels=[0,1,0,0],fontsize=10,linewidth=0.0)
# draw meridians
meridians = np.arange(180.,360.,3)
map1.drawmeridians(meridians,labels=[0,0,0,1],fontsize=10,linewidth=0.0)
x,y=map1(lon_point,lat_point)
#Draw contour lines and temperature maps
plt.plot(x,y,'ro')
plt.savefig(output_path+'contour_depth_tem_doppio2.png',dpi=300)
plt.show()
