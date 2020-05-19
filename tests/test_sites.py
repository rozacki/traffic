from common import *
from maos.sites import *

logger = get_logger()


def test_get_road_sites():
    sites = get_road_sites('sites_catalog.csv', 'M4')
    assert len(sites) == 848
    assert str(sites[1]) == "{'Description': 'M4/2295A2', 'Id': 1, 'Latitude': 51.49301153671121, " \
                            "'LegacyMeasurementSiteID': 30027234.0, 'Longitude': -0.520379557723297, " \
                            "'MeasurementSiteID': 1.0, 'MeasurementSiteName': 'M4 westbound between J4B " \
                            "and J5', 'Name': 'MIDAS site at M4/2295A2 priority 1 on link 105009001; GPS " \
                            "Ref: 502816;178156; Westbound', 'Status': 'Inactive', 'direction': " \
                            "'Westbound', 'gps_x': 502816.0, 'gps_y': 178156.0, 'link': '105009001', " \
                            "'road': 'M4', 'site_type': 'MIDAS'}"

    assert str(sites[24]) == "{'Description': 'M4/3479A', 'Id': 24, 'Latitude': 51.5206446979317, " \
                             "'LegacyMeasurementSiteID': 30028493.0, 'Longitude': -2.04923708882077, " \
                             "'MeasurementSiteID': 24.0, 'MeasurementSiteName': 'M4 westbound between J16 " \
                             "and J17', 'Name': 'MIDAS site at M4/3479A priority 1 on link 102004601; GPS " \
                             "Ref: 396680;180193; Westbound', 'Status': 'Active', 'direction': " \
                             "'Westbound', 'gps_x': 396680.0, 'gps_y': 180193.0, 'link': '102004601', " \
                             "'road': 'M4', 'site_type': 'MIDAS'}"


def test_get_sites():
    sites = get_sites('sites_catalog.csv', 1, 10)
    assert type(sites) == type(dict())
    assert len(sites) == 10
    assert str(sites[1]) == "{'Description': 'M4/2295A2', 'Id': 1, 'Latitude': 51.49301153671121, " \
                            "'LegacyMeasurementSiteID': 30027234.0, 'Longitude': -0.520379557723297, " \
                            "'MeasurementSiteID': 1.0, 'MeasurementSiteName': 'M4 westbound between J4B " \
                            "and J5', 'Name': 'MIDAS site at M4/2295A2 priority 1 on link 105009001; GPS " \
                            "Ref: 502816;178156; Westbound', 'Status': 'Inactive', 'direction': " \
                            "'Westbound', 'gps_x': 502816.0, 'gps_y': 178156.0, 'link': '105009001', " \
                            "'road': 'M4', 'site_type': 'MIDAS'}"


def test_get_link_sites():
    sites = get_link_sites('sites_catalog.csv', '123013701')
    assert type(sites) == type(dict())
    assert len(sites) == 12
    assert str(sites[86]) == "{'Description': 'M1/3966A', 'Id': 86, 'Latitude': 52.9395949413968, " \
                             "'LegacyMeasurementSiteID': 30031844.0, 'Longitude': -1.28544465160147, " \
                             "'MeasurementSiteID': 86.0, 'MeasurementSiteName': 'M1 northbound between J25 " \
                             "and J26', 'Name': 'MIDAS site at M1/3966A priority 1 on link 123013701; GPS " \
                             "Ref: 448119;338259; Northbound', 'Status': 'Active', 'direction': " \
                             "'Northbound', 'gps_x': 448119.0, 'gps_y': 338259.0, 'link': '123013701', " \
                             "'road': 'M1', 'site_type': 'MIDAS'}"
