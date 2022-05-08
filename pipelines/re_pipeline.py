# Import modules/methods
import pandas as pd

# Read in data
redfin = pd.read_csv('https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz', sep = '\t')
re_inv = pd.read_csv('https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/RDC_Inventory_Core_Metrics_County_History.csv')
re_hot = pd.read_csv('https://econdata.s3-us-west-2.amazonaws.com/Reports/Hotness/RDC_Inventory_Hotness_Metrics_County_History.csv')

# Drop last rows of Realtor.com data (junk lines; no real data)
re_inv.drop(re_inv.tail(1).index, inplace=True)
re_hot.drop(re_hot.tail(1).index, inplace=True)

# Fields to keep from Redfin
keep_redfin = ['period_begin', 'region', 'property_type', 'median_list_price', 'median_sale_price'
                , 'homes_sold', 'pending_sales', 'new_listings', 'inventory', 'avg_sale_to_list', 'price_drops', 'sold_above_list', 'median_dom']

# Fields to keep from Realtor.com inventory data
keep_re_inv = ['month_date_yyyymm', 'county_fips', 'county_name', 'median_listing_price', 'pending_listing_count', 'new_listing_count', 'total_listing_count'
                , 'price_reduced_count', 'median_days_on_market']

# Fields to keep from Realtor.com hotness data
keep_re_hot = ['month_date_yyyymm', 'county_fips', 'county_name', 'hotness_rank', 'hotness_score', 'supply_score'
                , 'demand_score', 'ldp_unique_viewers_per_property_mm']

# Drop unwanted fields
redfin = redfin[keep_redfin]
re_inv = re_inv[keep_re_inv]
re_hot = re_hot[keep_re_hot]

# Rename fields
rename_redfin = ['year_month', 'county_name', 'property_type', 'median_list_price', 'median_sale_price', 'homes_sold', 'pending_sales', 'new_listings'
                , 'total_inventory', 'avg_sale_to_list', 'price_drops', 'sold_above_list', 'median_dom']
rename_re_inv = ['year_month', 'county_fips', 'county_name', 'median_list_price', 'pending_sales', 'new_listings', 'total_inventory', 'price_drops', 'median_dom']
rename_re_hot = ['year_month', 'county_fips', 'county_name', 'hotness_rank', 'hotness_score', 'supply_score', 'demand_score', 'unq_viewers_mm']

redfin.columns = rename_redfin
re_inv.columns = rename_re_inv
re_hot.columns = rename_re_hot

# Clean up text fields
redfin['county_name'] = redfin['county_name'].str.upper()
re_inv['county_name'] = re_inv['county_name'].str.upper()
re_hot['county_name'] = re_hot['county_name'].str.upper()

redfin['property_type'] = redfin['property_type'].str.upper()

# Convert to Parquet
redfin.to_parquet('redfin.parquet')
re_inv.to_parquet('realtor_inventory.parquet')
re_hot.to_parquet('realtor_hotness.parquet')