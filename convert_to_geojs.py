import argparse

from pydruid.db import connect
from common import get_logger


logger = get_logger

geojson_format_open = '{ "type": "FeatureCollection", "features": [ '
geojson_format_body = '{{ "type": "Feature", "geometry": {{ "type": "Point", "coordinates": [ {lat}, {long}] }},' \
                      ' "properties": {{ "Avg Spd": "{avg_speed}", "site name":"{site_name}" }} }}'
geojson_format_close = '] }'


def get_sites( v):
    conn = connect(host='192.168.8.1', port=8082, path='/druid/v2/sql/', scheme='http')
    curs = conn.cursor()

    # column names must follow python class name convention otherwise they will be converted into _numbers
    curs.execute(
        f'SELECT "0 - 520 cm" lenght1_vehicles , "1160+ cm" lenght4_vehicles, "521 - 660 cm" lenght2_vehicles, '
        f'"661 - 1160 cm" lenght3_vehicles, "Avg mph" avg_speed, "Description", "Id", "Latitude", "Longitude", "Name", '
        f'"Report Date" report_date, "Site Name" site_name, "Status", "Time Interval" time_interval, '
        f'"Time Period Ending" time_period_ending, "Total Volume" total_volumne, '
        f'__time report_date_time FROM "site_8188" '
        f'where __time=TIMESTAMP \'2016-01-01 00:14:00.000\'')
    first_row = True
    comma = ''
    print(geojson_format_open)
    for row in curs:
        if first_row:
            first_row = False
        else:
            print(',')

        print(geojson_format_body.format(lat=row.Latitude, long=row.Longitude, avg_speed=row.avg_speed,
                                         site_name=row.site_name))
    print(geojson_format_close)


get_sites(date='2016-01-01 00:14:00.000')