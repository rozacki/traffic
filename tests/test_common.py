from common import *

logger = get_logger()


def test_get_road_sites():
    sites = get_road_sites('sites_enriched_roads.csv', 'M4')
    assert len(sites) == 848
    assert str(sites[1]) == "{'Id': 1, 'Name': 'MIDAS site at M4/2295A2 priority 1 on link 105009001;" \
                            " GPS Ref: 502816;178156; Westbound', " \
                            "'Description': 'M4/2295A2', 'Longitude': -0.520379557723297, " \
                            "'Latitude': 51.49301153671121, " \
                            "'Status': 'Inactive', 'MeasurementSiteName': 'M4 westbound between J4B and J5', " \
                            "'MeasurementSiteID': 1.0, 'LegacyMeasurementSiteID': 30027234.0, 'road': 'M4'}"

    assert str(sites[24]) == "{'Id': 24, 'Name': 'MIDAS site at M4/3479A priority 1 on link 102004601; GPS Ref:" \
                             " 396680;180193; Westbound', " \
                             "'Description': 'M4/3479A', 'Longitude': -2.04923708882077, " \
                             "'Latitude': 51.5206446979317, 'Status': 'Active', " \
                             "'MeasurementSiteName': 'M4 westbound between J16 and J17', 'MeasurementSiteID': 24.0, " \
                             "'LegacyMeasurementSiteID': 30028493.0, 'road': 'M4'}"
