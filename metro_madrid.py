import json
import geojson
import pandas as pd
import urllib.request

pd.set_option('display.width', 180)
pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 20)
pd.set_option('display.max_colwidth', 40)

# PATHs
metro_accesos_url_json = 'https://services5.arcgis.com/UxADft6QPcvFyDU1/arcgis/rest/services/Red_Metro/' + \
                         'FeatureServer/1/query?where=1%3D1&outFields=*&outSR=4326&f=json'
metro_accesos_geojson = 'https://opendata.arcgis.com/datasets/f3859438e5504a6b9ca745880f72ef1b_1.geojson'

with urllib.request.urlopen(metro_accesos_url_json) as url:
    data_metro_accesos = json.loads(url.read().decode())
    print(data_metro_accesos.keys())
    data = data_metro_accesos['features']

print(pd.json_normalize(data))
print(data[0].keys())

#with urllib.request.urlopen(metro_accesos_geojson) as url:
#    gj = geojson.load(url)
#    print(type(gj))
