#!/usr/bin/env python
# coding: utf-8

# In[1]:


# This Program tkaes data from the US Census and Raj Chetty's oppurtunity Index and cmbj-ines it with Shapefiles in order to create rich, multiayered interactive maps using folium.

tract_vars=pd.read_csv('tract_covariates.csv')
tract_vars.columns
tract_vars['cz'].astype('string')

#Load in the Raj Chetty Dataset


# In[3]:


import fiona
from shapely.geometry.point import Point

# Packages to read ShpaeFiles


# In[2]:



import pandas as pd
import numpy as np
import geopy
import geopandas as gpd
import mapclassify
import matplotlib.pyplot as plt
from scipy import stats
import json

from bokeh.io import output_notebook, show, output_file
from bokeh.plotting import figure
from bokeh.models import GeoJSONDataSource, LinearColorMapper, ColorBar, NumeralTickFormatter
from bokeh.palettes import brewer

from bokeh.io.doc import curdoc
from bokeh.models import Slider, HoverTool, Select
from bokeh.layouts import widgetbox, row, column

from geopy import geocoders
import folium

from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded
import time
import requests
import fsspec
from io import StringIO


# In[3]:



countries_gdf = gpd.read_file("Tract_2010Census_DP1.shp",SHAPE_RESTORE_SHX='Yes')
co_gdf = gpd.read_file("Tract_2010Census_DP1.dbf")

#Load in the ShapeFile for the 2010 Census Tracts


# In[4]:



tract_vars=pd.read_csv('tract_covariates.csv')
tract_vars['tract_id']=tract_vars['tract'].astype('int')
tract_vars[tract_vars['tract_id']==9503]
tract_vars['state'] = tract_vars['state'].astype('string').apply(lambda x: x.zfill(2))
tract_vars['county'] = tract_vars['county'].astype('string').apply(lambda x: x.zfill(3))
tract_vars['tract'] = tract_vars['tract'].astype('string').apply(lambda x: x.zfill(6))
tract_vars['fips']=tract_vars['state']+tract_vars['county']+tract_vars['tract']
tract_vars['fips_int']=tract_vars['fips'].astype('int64')
tract_vars

#Combine the County, State and Tract code Columns into one Tract level FIPS code


# In[11]:


co_gdf['tract'] = co_gdf['NAMELSAD10'].map(lambda x: x.lstrip('Census Tract'))

co_gdf['tract_id']=co_gdf['tract'].astype('float').astype('int64')

co_gdf['fips']=co_gdf['GEOID10'].astype('int64')


raj_2010_shape = co_gdf.merge(tract_vars,how='right',
left_on=['fips'], 
right_on=['fips_int'])
raj_2010_shape.shape
#Merge the ShapeFile and the Raj Chetty data using the FIPS code


# In[28]:


census_codes=pd.read_excel('ExcelCensusTractReference2010.xls')
cc=pd.read_csv('nb to wy.csv')
ci=pd.read_csv('island.csv')
census_list=[census_codes,cc,ci]
census=pd.concat(census_list)
census[(census['countyname']=='Falls Church city')| (census['tractname']== "Census Tract 2019")]
#Load in the Names of the States and Counties for each census tract corresponding with the FIPS


# In[16]:


final = raj_2010_shape.merge(census,how='right',
left_on=['fips_int'], 
right_on=['tract_fips'])
final.shape


#Merge the names into the larger dataset


# In[39]:


final = final.merge(urban_inst, how='left',
left_on='fips_int', 
right_on='GEOID',suffixes=('', '_y'))
final.drop(final.filter(regex='_y$').columns.tolist(),axis=1, inplace=True)
final.drop(final.filter(regex='_x$').columns.tolist(),axis=1, inplace=True)
final.drop(columns={'ALAND10', 'AWATER10', 'INTPTLAT10', 'INTPTLON10','DP0010005', 'DP0010006', 'DP0010007', 'DP0010008', 'DP0010009', 'DP0010010', 'DP0010011', 'DP0010012', 'DP0010013', 'DP0010014', 'DP0010015', 'DP0010016', 'DP0010017', 'DP0010018', 'DP0010019', 'DP0010020', 'DP0010021', 'DP0010022', 'DP0010023', 'DP0010024', 'DP0010025', 'DP0010026', 'DP0010027', 'DP0010028', 'DP0010029', 'DP0010030', 'DP0010031', 'DP0010032', 'DP0010033', 'DP0010034', 'DP0010035', 'DP0010036', 'DP0010037', 'DP0010038', 'DP0010039', 'DP0010040', 'DP0010041', 'DP0010042', 'DP0010043', 'DP0010044', 'DP0010045', 'DP0010046', 'DP0010047', 'DP0010048', 'DP0010049', 'DP0010050', 'DP0010051', 'DP0010052', 'DP0010053', 'DP0010054', 'DP0010055', 'DP0010056', 'DP0010057', 'DP0020001', 'DP0020002', 'DP0020003', 'DP0030001', 'DP0030002', 'DP0030003', 'DP0040001', 'DP0040002', 'DP0040003', 'DP0050001', 'DP0050002', 'DP0050003', 'DP0060001', 'DP0060002', 'DP0060003', 'DP0070001', 'DP0070002', 'DP0070003', 'DP0080001', 'DP0080002', 'DP0080003', 'DP0080004', 'DP0080005', 'DP0080006', 'DP0080007', 'DP0080008', 'DP0080009', 'DP0080010', 'DP0080011', 'DP0080012', 'DP0080013', 'DP0080014', 'DP0080015', 'DP0080016', 'DP0080017', 'DP0080018', 'DP0080019', 'DP0080020', 'DP0080021', 'DP0080022', 'DP0080023', 'DP0080024', 'DP0090001', 'DP0090002', 'DP0090003', 'DP0090004', 'DP0090005', 'DP0090006', 'DP0100001', 'DP0100002', 'DP0100003', 'DP0100004', 'DP0100005', 'DP0100006', 'DP0100007', 'DP0110001', 'DP0110002', 'DP0110003', 'DP0110004', 'DP0110005', 'DP0110006', 'DP0110007', 'DP0110008', 'DP0110009', 'DP0110010', 'DP0110011', 'DP0110012', 'DP0110013', 'DP0110014', 'DP0110015', 'DP0110016', 'DP0110017', 'DP0120001', 'DP0120002', 'DP0120003', 'DP0120004', 'DP0120005', 'DP0120006', 'DP0120007', 'DP0120008', 'DP0120009', 'DP0120010', 'DP0120011', 'DP0120012', 'DP0120013', 'DP0120014', 'DP0120015', 'DP0120016', 'DP0120017', 'DP0120018', 'DP0120019', 'DP0120020', 'DP0130001', 'DP0130002', 'DP0130003', 'DP0130004', 'DP0130005', 'DP0130006', 'DP0130007', 'DP0130008', 'DP0130009', 'DP0130010', 'DP0130011', 'DP0130012', 'DP0130013', 'DP0130014', 'DP0130015', 'DP0140001', 'DP0150001', 'DP0160001', 'DP0170001', 'DP0180001', 'DP0180002', 'DP0180003', 'DP0180004', 'DP0180005', 'DP0180006', 'DP0180007', 'DP0180008', 'DP0180009', 'DP0190001', 'DP0200001', 'DP0210001', 'DP0210002', 'DP0210003', 'DP0220001', 'DP0220002', 'DP0230001', 'DP0230002','hhinc_mean2000', 'mean_commutetime2000', 'frac_coll_plus2000','med_hhinc1990','popdensity2000','poor_share2000', 'poor_share1990','share_black2000', 'share_white2000', 'share_hisp2000', 'share_asian2000','singleparent_share1990', 'singleparent_share2000','NAMELSAD10','tractname'},inplace=True)
print(final.columns.tolist())
final.shape
#Drop uneeded columns


# In[77]:



final=pd.read_csv('Tract_Level_Data_ESG.csv')
systima_properties=pd.read_csv('systima_geocoded.csv')



dc_counties=['Montgomery County', 'Prince George County', 'Arlington County', 'Fairfax County', 'Prince William County','District of Columbia']
dc_msa_states=['Virginia','Maryland', 'District of Columbia']
counties=['New York County']
states=['District of Columbia']
states_abb = ['DC']
counties_abb=['District of Columbia County','District Of Columbia County']
bay_area_counties= ['District of Columbia']
utah = ['Salt Lake County','Summit County']
#Lists of specific counties we want to look at to filter the dataset


# In[8]:


dc=final[final['countyname'].isin(dc_counties) & final['statename'].isin(dc_msa_states)]
bay_area=final[final['countyname'].isin(bay_area_counties) & final['statename'].isin(states)]
ca=final[final['countyname'].isin(counties) & final['statename'].isin(states)]
ut=final[final['countyname'].isin(utah) & final['statename'].isin(states)]
#Split the larger dataset with these filters
gdf_bay.geometry.type


# In[9]:


gdf_dc = gpd.GeoDataFrame(dc, geometry='geometry')
gdf_ca = gpd.GeoDataFrame(ca, geometry='geometry')
gdf_bay = gpd.GeoDataFrame(bay_area, geometry='geometry')
gdf_ut=gpd.GeoDataFrame(ut, geometry='geometry')
gdf_final=gpd.GeoDataFrame(final, geometry='geometry')
print(gdf_bay.columns.tolist())
# Convert These into GeoDataFrames


# In[10]:



#gdf_bay.to_file("bay.GeoJSON", driver="GeoJSON",geometry='geometry')

gdf_bay = gdf_bay.filter(['geometry','total_index_quantile','housing_index_quantile','covid_index_quantile','GEOID10','tract','equity_index_quantile','med_hhinc2016','share_black2010'])
print(gdf_bay.columns.tolist())
#gdf_bay.to_file("bay.GeoJSON", driver="GeoJSON",geometry='geometry')
gdf_bay

#Save them to few different file types that can be used later


# In[11]:


from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
locator = Nominatim(user_agent= 'myGeocoder')
location = locator.geocode('Champ de Mars, Paris, France')
print('Latitude = {}, Longitude = {}'.format(location.latitude, location.longitude))

# Import the Geocoding package and run a quick test to make sure it works.


# In[12]:


Systima = pd.read_csv('Address OBAL.csv')
Systima.head()
Systima['Address']=Systima['Street Address']+','+Systima['Property City']+','+Systima['Property State']
Systima['Address']=Systima['Address'].str.lstrip(' ')
Systima[Systima['Property State']=='MD']
#Load in the data for Systima Properties


# In[52]:


#Geocode the Systima Data. Takes several minutes to run.

# 1 - conveneint function to delay between geocoding calls
geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
# 2- - create location column
Systima['location'] = Systima['Address'].apply(geocode)
# 3 - create longitude, latitude and altitude from location column (returns tuple)
Systima['point'] = Systima['location'].apply(lambda loc: tuple(loc.point) if loc else None)
# 4 - split point column into latitude, longitude and altitude columns
Systima[['latitude', 'longitude', 'altitude']] = pd.DataFrame(Systima['point'].tolist(), index=Systima.index)


# In[133]:


#The last 36 Properties were all geocoded incorrectly, throwing them out until I can find a better API
map1 = folium.Map(
   location=[59.338315,18.089960],
  tiles='cartodbpositron',
   zoom_start=12,
)
systima_properties.apply(lambda row:folium.Marker(location=[row["latitude"], row["longitude"]]).add_child(folium.Popup(row['Reference Obligation Name'])).add_to(map1), axis=1)
map1
#gdf_bay[gdf_bay['GEOID10']=='12086010617']


# In[97]:



systima_properties=pd.read_csv('systima_geocoded.csv')
#df1 = Systima.filter(items=(['Reference Obligation Name','Street Address','Property City','Property State']))
#df2= systima_props_final.filter(items=(['Reference Obligation Name','Street Address','Property City','Property State']))
#df = df1.merge(df2, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
#df

#42 of the 353 properties in df failed to GeoCode. Looking for a solution might try MapQuest API 


systima_properties['County']=systima_properties['County']+ ' County'
sys_metro =systima_properties[systima_properties['County'].isin(counties_abb) & systima_properties['Property State'].isin(states_abb)]
#sys_metro
#systima_properties[(systima_properties['Reference Obligation Name']=='Dahlgreen Courts Apartments') | (systima_properties['Reference Obligation Name']=='Livingston Place at Southern')]
#gdf_bay['tract']
systima_properties


# In[14]:


#Load in HUD dataset of LIHTC proprties and elminate old properties and NANs

HUD= pd.read_csv('LIHTCPUB.csv')

HUD_current=HUD[HUD['yr_pis']>2004]
HUD_current.drop(HUD_current[(HUD_current['yr_pis']==8888) | (HUD_current['yr_pis']==9999)].index,inplace=True)


# In[94]:




final['GEOID10']=final['GEOID10'].astype('string')

HUD_full['fips2010']=HUD_full['fips2010'].astype('string')
HUD_full['fips2010'] = HUD_full['fips2010'].replace('X', '0')

HUD_full=HUD_current.merge(final, how='left', left_on = 'fips2010', right_on = 'GEOID10')

#Combine HUD data with census tract data


# In[33]:


HUD_full['poor_share2010'].mean()
final['poor_share2010'].mean()
final['geo']=final['geometry'].astype('object')
final['geo'].dtype

#run some summary stats


# In[98]:


systima_properties
point = systima_properties.filter(items = ['latitude','longitude'])

lat = point['latitude'].astype("str").tolist()
lng = point['longitude'].astype("str").tolist()

responses=[]

#prepare the data for the FCC API


# In[101]:


#Fetches data from the FCC census block API. Also can take a while to run.
def get_responses(lat,lng):
    
    for i in range(len(lat)):
        response = requests.get("https://geo.fcc.gov/api/census/block/find?latitude="+lat[i]+"%20&longitude="+lng[i]+"&format=json")
        responses.append(response.json())

    return responses

responses=get_responses(lat,lng)


# In[102]:


#dump the block into JSON

FIPS= json.dumps(responses)
j=pd.read_json(FIPS)
block=j['Block']
b=pd.DataFrame(block)
Rating = b['Block'].values.tolist()
rate = pd.DataFrame(Rating,columns =['FIPS'])


# In[103]:


#covert the block level fip id into tract level id
rate=pd.read_csv('systima_tracts.csv')
#len(rate['FIPS'][0])
final['GEOID10'].tolist().index('36005017901')

#rate['fips_tract'] = rate['FIPS'].str.strip().str[0:11]
#rate.drop(columns={'FIPS'},inplace=True)
rate.to_csv('systima_tracts.csv')
rate


# In[2]:


syst=systima_properties.merge(rate, how='left', left_on = systima_properties.index, right_on=rate.index)
final['geoid_float']=final['GEOID10'].astype('float64')
systima_full=syst.merge(final, how='left', left_on = 'fips_tract', right_on = 'geoid_float')
systima_full
systima_full.to_csv('systima_tract_data.csv')
HUD_full.to_csv('HUD_tract_data.csv')

#Join the FCC API data to the Systima data. Now we know what census tract each Systima property is in based on its Geocoded LNG and LAT


# In[38]:


info_SYS = systima_properties.filter(items = ['latitude','longitude','Street Address','Property City','Property State'])

lat = info_SYS['latitude'].astype("str").tolist()
lng = info_SYS['longitude'].astype("str").tolist()
address = info_SYS['Street Address'].astype("str").tolist()
city = info_SYS['Property City'].astype("str").tolist()
state= info_SYS['Property State'].astype("str").tolist()
scores=[]
API_key = 'f384a61966d2f2600f6ee1c13849fa6f'
response = requests.get('https://api.walkscore.com/score?format=json&address='+address[0]+'%20'+city[0]+'%20'+state[0]+'%2098101&lat='+lat[0]+'&lon='+lng[0]+'&transit=0&bike=0&wsapikey='+API_key)
print(response.json())
info_SYS
#Prperare the Systima data for the WalkScore API and run a test on the first row


# In[32]:


#Fetches WalkScore data with the API. Note: This can take long time to run. Also max calls allowed per day is 5000.
def walk_score(lat,lng,address,city,state):
    
    for i in range(len(lat)):
        response = requests.get('https://api.walkscore.com/score?format=json&address='+address[i]+'%20'+city[i]+'%20'+state[i]+'%2098101&lat='+lat[i]+'&lon='+lng[i]+'&transit=0&bike=0&wsapikey='+API_key)
        scores.append(response.json())

    return scores

#info=walk_score(lat,lng,address,city,state)


# In[40]:


#Puts WalkScore data into JSON and runs summary stats
#info_SYS_WALK=walk_score(lat,lng,address,city,state)
#walk_scores= json.dumps(info_SYS_WALK)
#walk_score_sys_final=pd.read_json(StringIO(walk_scores))
walk_score_sys_final=pd.read_csv('systima_walkscores.csv')
walk_score_sys_final.to_csv('systima_walkscores.csv')

systima_full.drop(columns={'key_0','altitude'},inplace=True)
systima_full.reset_index(drop=True,inplace=True)
systima_fin=systima_full.merge(walk_score_sys_final['walkscore'], how='left',left_on = systima_full.index,right_on=walk_score_sys_final.index)
systima_fin


# In[43]:


#perapres HUD LIHTC data to be fed into API
info_HUD = HUD_full.filter(items = ['proj_add','proj_cty' ,'proj_st','latitude','longitude']).iloc[5000:8000]
lat = info_HUD['latitude'].astype("str").tolist()
lng = info_HUD['longitude'].astype("str").tolist()
address = info_HUD['proj_add'].astype("str").tolist()
city = info_HUD['proj_cty'].astype("str").tolist()
state= info_HUD['proj_st'].astype("str").tolist()
len(lat)
API_key = 'f384a61966d2f2600f6ee1c13849fa6f'
#HUD_walk_8000=walk_score(lat,lng,address,city,state)
#len(lat)


# In[44]:


walk_scores_HUD_5000 = json.dumps(info)
walk_scores_HUD_1 = pd.read_json(StringIO(walk_scores_HUD_5000))
walk_scores_HUD_1['walkscore'].mean()


# In[45]:


walk_scores_HUD_1.to_csv('walkscores_HUD.csv')


# In[29]:






# In[46]:


systima_fin['Property State']=systima_fin['Property State'].str.strip(' ')

a=systima_fin.groupby('Property State').mean().filter(['frac_coll_plus2010','rent_twobed2015','ln_wage_growth_hs_grad'])
a.reset_index(inplace=True)


# In[56]:


b=HUD_full.groupby('proj_st').mean().filter(['frac_coll_plus2010','rent_twobed2015','ln_wage_growth_hs_grad'])
b.reset_index(inplace=True)
b
systima_fin


# In[48]:


urban_inst = pd.read_csv('housing_index_state_adj.csv')

#systima_urban=urban_inst.merge(systima_fin, how='right', left_on='GEOID',right_on='geoid_float')
#b=systima_urban.groupby('Property State').mean().filter(['total_index_quantile','covid_index_quantile'])
#b=b.reset_index()
#HUD_full


# In[57]:


hud_urban=urban_inst.merge(HUD_full, how='right', left_on='GEOID',right_on='tract_fips')
a=hud_urban.groupby('proj_st').mean().filter(['total_index_quantile','covid_index_quantile'])
a=a.reset_index()


# In[54]:


summary=b.merge(a,how='left',left_on= 'Property State',right_on= 'proj_st')
summary.columns = summary.columns.str.replace("_x", "_systima")
summary.columns = summary.columns.str.replace("_y", "_HUD")


# In[95]:


HUD_full['total_index_quantile'].mean()


# In[96]:


systima_urban['total_index_quantile'].mean()


# In[108]:


4358.739758235294/2335.3928193912034


# In[68]:


sys_urban_gdf = gpd.GeoDataFrame(systima_urban, geometry='geometry')


# In[35]:


from branca.element import MacroElement

from jinja2 import Template

class BindColormap(MacroElement):
    """Binds a colormap to a given layer.

    Parameters
    ----------
    colormap : branca.colormap.ColorMap
        The colormap to bind.
    """
    def __init__(self, layer, colormap):
        super(BindColormap, self).__init__()
        self.layer = layer
        self.colormap = colormap
        self._template = Template(u"""
        {% macro script(this, kwargs) %}
            {{this.colormap.get_name()}}.svg[0][0].style.display = 'block';
            {{this._parent.get_name()}}.on('overlayadd', function (eventLayer) {
                if (eventLayer.layer == {{this.layer.get_name()}}) {
                    {{this.colormap.get_name()}}.svg[0][0].style.display = 'block';
                }});
            {{this._parent.get_name()}}.on('overlayremove', function (eventLayer) {
                if (eventLayer.layer == {{this.layer.get_name()}}) {
                    {{this.colormap.get_name()}}.svg[0][0].style.display = 'none';
                }});
        {% endmacro %}
        """)  # noqa
# Build a Custom class to allow for multiple Color Maps in one file.

# In[1]:


from branca.colormap import linear
gdf_bay.set_crs(epsg=4326,inplace=True)
m = folium.Map([38.9072,-77.0369], zoom_start=6,tiles='pk.eyJ1IjoiY2hyaXN0aWFudGF5bG9yMTk5NyIsImEiOiJja3FiZjA1YzUwMGE2MnBxcjhwaXVmdW14In0.B1QUQs22K92RZEfaWXm-Ug')

#Build the map and legend with folium, load in a custom tileset.

import branca
colors = ["YlGn","OrRd","BuPu","GnBu","OrRd","YlGn"]
cmap1 = branca.colormap.StepColormap(
        colors=['#ffffcc','#d9f0a3','#addd8e','#78c679','#238443'],
        vmin=0,
        vmax=gdf_bay['total_index_quantile'].max(),  
        caption='Total Index Quantile')

cmap2 = branca.colormap.StepColormap(
        colors=["#fef0d9",'#fdcc8a','#fc8d59','#d7301f'],
        vmin=0,
        vmax=gdf_bay['housing_index_quantile'].max(),  
        caption='Housing Index quantile')
        
cmap3 = branca.colormap.StepColormap(
        colors=branca.colormap.step.BuPu_09.colors,
        vmin=0,
        vmax=gdf_bay['covid_index_quantile'].max(),  
        caption='Covid Index Quantile')
        
cmap4 = branca.colormap.StepColormap(
        colors=branca.colormap.step.GnBu_09.colors,
        vmin=0,
        vmax=gdf_bay['equity_index_quantile'].max(),  
        caption='Equity Index Quantile')

cmap5 = branca.colormap.StepColormap(
        colors=["#fef0d9",'#fdcc8a','#fc8d59','#d7301f'],
        vmin=0,
        vmax=gdf_bay['med_hhinc2016'].max(),  
        caption='Median Income 2016')

cmap6 = branca.colormap.StepColormap(
        colors=['#ffffcc','#d9f0a3','#addd8e','#78c679','#238443'],
        vmin=0,
        vmax=gdf_bay['share_black2010'].max(),  
        caption='Share Black 2010')

cmaps = [cmap1, cmap2,cmap3,cmap4,cmap5,cmap6]
    
total = bay_area['total_index_quantile']
housing = bay_area['housing_index_quantile']
covid= bay_area['covid_index_quantile']
equity = bay_area['equity_index_quantile']
med_inc=bay_area['med_hhinc2016']
blk=bay_area['share_black2010']


columns=['total_index_quantile','housing_index_quantile','covid_index_quantile','equity_index_quantile','med_hhinc2016','share_black2010']



sys_metro.apply(lambda row:folium.Marker(location=[row["latitude"], row["longitude"]]).add_child(folium.Popup(row['Reference Obligation Name'])).add_to(m), axis=1)



for color, cmap, i in zip(colors, cmaps, columns):
    choropleth = folium.Choropleth(
    geo_data=gdf_bay,
    name=i,
    data=bay_area,
    columns=['GEOID10', i],
    key_on='feature.properties.GEOID10',
    fill_color=color,
    nan_fill_color="black",
    fill_opacity=0.7,
    line_opacity=0.2,
    colormap=cmap,
    show=False,
    line_color='black')
     # this deletes the legend for each choropleth you add
    for child in choropleth._children:
        if child.startswith("color_map"):
            del choropleth._children[child]
            
   
    style_function1 = lambda x: {'fillColor': '#ffffff', 
                            'color':'#000000', 
                            'fillOpacity': 0.1, 
                            'weight': 0.1}
    highlight_function1 = lambda x: {'fillColor': '#000000', 
                                'color':'#000000', 
                                'fillOpacity': 0.50, 
                                'weight': 0.1}
    NIL1 = folium.features.GeoJson(
            data = gdf_bay,
            style_function=style_function1, 
            control=False,
            highlight_function=highlight_function1, 
            tooltip=folium.features.GeoJsonTooltip(
            fields= ['GEOID10','total_index_quantile','housing_index_quantile',
                    'equity_index_quantile','med_hhinc2016','share_black2010'
                    ],
            aliases= ['GEOID10','total_index_quantile','housing_index_quantile',
                    'equity_index_quantile','med_hhinc2016','share_black2010'
                    ],
            style=("background-color: white; color: #333333; font- family: arial; font-size: 12px; padding: 10px;") 
            )
        )            
   
 
    

    m.add_child(NIL1)
    m.keep_in_front(NIL1)        # add cmap to `sample_map`
    m.add_child(cmap)
                
        # add choropleth to `sample_map`
    m.add_child(choropleth)
        
        # bind choropleth and cmap
    bc = BindColormap(choropleth, cmap)
        
        # add binding to `m`
    m.add_child(bc)
    
folium.TileLayer('cartodbdark_matter',control=True,name="dark mode").add_to(m)
folium.TileLayer('cartodbpositron',control=True,name="light mode").add_to(m)
folium.LayerControl(collapsed=True).add_to(m)
    
m
m.save('Distriect of Columbia.html')


# In[33]:


final.to_csv('Tract_Level_Data_ESG.csv')


# In[15]:


cal=pd.read_csv('CalHFA Series 2019 - 2 Data Tape_FINAL 11.26.19.csv', header=1)
cal=cal.dropna(axis=1,how ='all')
cal=cal.dropna(axis=0,how ='all')
#print(cal.columns.tolist())
cal['Address']=cal['Street Address']+','+cal['Property City']+','+cal['Property\nState']
cal


# In[2]:


from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
locator = Nominatim(user_agent= 'myGeocoder')
location = locator.geocode('Champ de Mars, Paris, France')
print('Latitude = {}, Longitude = {}'.format(location.latitude, location.longitude))


# In[16]:


# 1 - conveneint function to delay between geocoding calls
geocode = RateLimiter(locator.geocode, min_delay_seconds=1)
# 2- - create location column
cal['location'] = cal['Address'].apply(geocode)
# 3 - create longitude, latitude and altitude from location column (returns tuple)
cal['point'] = cal['location'].apply(lambda loc: tuple(loc.point) if loc else None)
# 4 - split point column into latitude, longitude and altitude columns
cal[['latitude', 'longitude', 'altitude']] = pd.DataFrame(cal['point'].tolist(), index=cal.index)


# In[17]:




# In[18]:




# In[28]:





