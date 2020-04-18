import argparse

from pydruid.db import connect
from common import get_logger


logger = get_logger

geojson_format_open = '{ "type": "FeatureCollection", "features": [ '
geojson_format_body = '{{ "type": "Feature", "geometry": {{ "type": "Point", "coordinates": [ {lang}, {lat}] }}, "properties": {{ "Avg Spd": "{avg_speed}", "site name":"{site_name}" }} }} '
geojson_format_close = '] }'


def get_sites(date):
    conn = connect(host='192.168.8.1', port=8082, path='/druid/v2/sql/', scheme='http')
    curs = conn.cursor()
    #curs.execute(f'SELECT Latitude, Longitude  FROM site_8188  where __time=TIMESTAMP "{date}"')
    curs.execute(f"SELECT Latitude, Longitude, \"Site Name\" from \"site_8188\" where __time=TIMESTAMP '2016-01-01 00:14:00.000'")
    print(geojson_format_open)
    for row in curs:
        print(geojson_format_body.format(lang=row[0], lat=row[1], avg_speed=74, site_name=row[2]) + ',')
    print(geojson_format_close)


get_sites(date='2016-01-01 00:14:00.000')
