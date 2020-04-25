'''
This script merges sites and conversion table into single csv file config/sites_enriched.csv
Data sources
- sites:
-- curl http://webtris.highwaysengland.co.uk/api/v1.0/sites
- conversion tables
-- curl http://tris.highwaysengland.co.uk/ConversionTable
'''

import os
from common import load_sites_info, configs_base_folder
import pandas as pd

sites = pd.DataFrame(load_sites_info()['sites'])
sites.set_index('Id')

conversion_table = pd.read_csv(os.path.join(configs_base_folder, 'ConversionTable.csv'))
conversion_table.set_index('MeasurementSiteID')
joined = sites.join(conversion_table, how='left')
joined.to_csv(os.path.join(configs_base_folder, 'sites_enriched.csv'), index=False)
