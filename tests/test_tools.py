from tools import *

test_enriched_file = 'test_enriched_file.csv'


def test_join_sites_and_conversions():
    df = join_sites_and_conversions(enriched_file=test_enriched_file)
    df.info()
    assert len(df) == 17851
    assert len(df.columns) == 9
    assert df.shape == (17851, 9)
    assert df.size == 160659
    assert list(df.columns) == ['Id', 'Name', 'Description', 'Longitude', 'Latitude', 'Status',
                                                 'MeasurementSiteName', 'MeasurementSiteID', 'LegacyMeasurementSiteID']


def test_add_road_name_column():
    df = add_road_name_column(test_enriched_file, output_file_name='test_sites_enriched_roads.csv')
    df.info()
    assert len(df) == 17851
    assert len(df.columns) == 10
    assert df.shape == (17851, 10)
    assert df.size == 178510
    assert list(df.columns) == ['Id', 'Name', 'Description', 'Longitude', 'Latitude', 'Status',
                                                 'MeasurementSiteName', 'MeasurementSiteID', 'LegacyMeasurementSiteID',
                                                 'road']