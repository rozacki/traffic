import argparse

from pydruid.db import connect
from common import get_logger
from geojson import Point, Feature, FeatureCollection, dump, dumps

logger = get_logger


def _druid_get_connection():
    # when are cursor closed?
    return connect(host='192.168.8.1', port=8082, path='/druid/v2/sql/', scheme='http')


def _druid_close_cursor(curs):
    curs.close()


def _druid_get_sites_snapshot(datasource, date):
    conn = _druid_get_connection()
    curs = conn.cursor()

    # column names must follow python class name convention otherwise they will be converted into _numbers
    curs.execute(
        f'SELECT "0 - 520 cm" lenght1_vehicles , "1160+ cm" lenght4_vehicles, "521 - 660 cm" lenght2_vehicles, '
        f'"661 - 1160 cm" lenght3_vehicles, "Avg mph" avg_speed, "Description", "Id", "Latitude", "Longitude", "Name", '
        f'"Report Date" report_date, "Site Name" site_name, "Status", "Time Interval" time_interval, '
        f'"Time Period Ending" time_period_ending, "Total Volume" total_volume, '
        f'__time report_date_time FROM "{datasource}" '
        f'where __time=TIMESTAMP %(date)s', {'date': date})

    return curs


def _druid_get_site_timeseries(datasource, site, startdate, enddate):
    conn = _druid_get_connection()
    curs = conn.cursor()

    # column names must follow python class name convention otherwise they will be converted into _numbers
    curs.execute(
        f'SELECT "0 - 520 cm" lenght1_vehicles , "1160+ cm" lenght4_vehicles, "521 - 660 cm" lenght2_vehicles, '
        f'"661 - 1160 cm" lenght3_vehicles, "Avg mph" avg_speed, "Description", "Id", "Latitude", "Longitude", "Name", '
        f'"Report Date" report_date, "Site Name" site_name, "Status", "Time Interval" time_interval, '
        f'"Time Period Ending" time_period_ending, "Total Volume" total_volume, '
        f'__time report_date_time FROM "{datasource}" '
        f'where __time>=TIMESTAMP %(startdate)s and __time<TIMESTAMP %(enddate)s '
        f'and \"Id\"=%(site)s', {'startdate': startdate, 'enddate': enddate, 'site': site})

    return curs


def get_sites_snapshot(datasource='site_8188', date='2016-01-01 00:14:00.000'):
    '''
    Based on hardcoded values convert druid result to geojson
    :return: geojs many sites single timestamp one feature  into stdout
    '''

    geojson_format_open = '{ "type": "FeatureCollection", "features": [ '
    geojson_format_body = '{{ "type": "Feature", "geometry": {{ "type": "Point", "coordinates": [ {lat}, {long}] }},' \
                          ' "properties": {{ "Avg speed": "{avg_speed}", "site name":"{site_name}" }} }}'
    geojson_format_close = '] }'

    curs = _druid_get_sites_snapshot(date, datasource)

    first_row = True
    print(geojson_format_open)
    for row in curs:
        if first_row:
            first_row = False
        else:
            print(',')

        print(geojson_format_body.format(lat=row.Latitude, long=row.Longitude, avg_speed=row.avg_speed,
                                         site_name=row.site_name))
    print(geojson_format_close)


def get_sites_snapshot2(datasource, date):
    curs = _druid_get_sites_snapshot(datasource, date)
    features = []
    for row in curs:
        point = Point((float(row.Latitude), float(row.Longitude)))
        features.append(Feature(geometry=point, properties={'Avg speed': row.avg_speed}))

    feature_collection = FeatureCollection(features)
    print(dumps(feature_collection))


def get_site_timeseries(datasource, site, startdate, enddate):
    curs = _druid_get_site_timeseries(datasource, site, startdate, enddate)
    features = []
    point_added = False
    avg_speeds = []
    for row in curs:
        if not point_added:
            point = Point((float(row.Latitude), float(row.Longitude)))
            point_added = True

        avg_speeds.append({'time': row.report_date_time, 'avg speed': row.avg_speed})

    features.append(Feature(geometry=point, properties={'times': avg_speeds}))

    feature_collection = FeatureCollection(features)
    print(dumps(feature_collection))

#get_sites_snapshot(datasource='200_sites_2016', date='2016-01-01 00:14:00.000')

#get_sites_snapshot2(datasource='200_sites_2016', date='2016-01-01 00:14:00.000')

get_site_timeseries(datasource='200_sites_2016', site=137, startdate='2016-01-01 00:14:00', enddate='2016-01-02 00:14:00')
