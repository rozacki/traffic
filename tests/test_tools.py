from tools import *

test_enriched_file = 'test_enriched_file.csv'


def test_join_sites_and_conversions():
    df = join_sites_and_conversions(enriched_file=test_enriched_file)
    df.info()
    assert len(df) == 17846
    assert len(df.columns) == 9
    assert df.shape == (17846, 9)
    assert df.size == 160614
    assert list(df.columns) == ['Id', 'Name', 'Description', 'Longitude', 'Latitude', 'Status',
                                                 'MeasurementSiteName', 'MeasurementSiteID', 'LegacyMeasurementSiteID']


def test_add_road_name_column():
    df = add_road_name_column(test_enriched_file, output_file_name='test_sites_catalog.csv')
    df.info()
    assert len(df) == 17846
    assert len(df.columns) == 15
    assert df.shape == (17846, 15)
    assert df.size == 267690
    assert list(df.columns).sort() == ['Id', 'Name', 'Description', 'Longitude', 'Latitude', 'Status',
                                                 'MeasurementSiteName', 'MeasurementSiteID', 'LegacyMeasurementSiteID',
                                                 'direction' ,'gps_x', 'gps_y', 'link', 'road', 'site_type'].sort()


def test_get_kv_from_name():
    name = 'MIDAS site at M1/3942A priority 1 on link 123013701; GPS Ref: 447146;336274; Northbound'
    kv = get_columns_from_name(1, name)
    assert len(kv) == 5
    assert kv['site_type'] == 'MIDAS'
    assert kv['link'] == '123013701'
    assert kv['gps_x'] == 447146
    assert kv['gps_y'] ==  336274
    assert kv['direction'] == 'Northbound'