"""
This file is responsible for the data cleansing of the annual-number-of-deaths-by-world-region file.
The following steps are applied:
- All records for continents are removed.
- Continent added to each country record.
"""

import pandas as pd
