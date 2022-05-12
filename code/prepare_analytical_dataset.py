import pandas as pd
import numpy as np
from utils import *

# Load the interpolated balanced panel data
df = pd.read_hdf('output/servers_panel_semibalanced.h5', 'df')

# Drop the data that has empty information since we don't use this in our analysis
df = df.loc[(df['server_name']!="")]
df = df.loc[df['server_name'].notnull()]

# Load the dates
df['dt'] = pd.to_datetime(df['dt'])
df['yr'] = df['dt'].dt.year
df['dm'] = df['yr'].astype(int)*100 + 1

# Subset the columns
df = df[['domain','yr','dm','dt','server_name','server_version','interpolated']]

# For each observed server version, add any information about it
# This includes when it first became available.
versions = versions = get_version_df()
versions.rename(columns={
    'name': 'server_name',
    'version': 'server_version'
}, inplace=True)
versions = versions.loc[
    (versions['server_name'].notnull()) & (versions['server_version'].notnull()) &
    (versions['server_name']!='') & (versions['server_version']!='')
    ]
df = pd.merge(df,versions,
    on=['server_name','server_version'],
    how='left')

# Find the most popular IIS version by each month and add that to every single observation.
# We use this to interpolate the unobserved value of the open source observations.
iis_popular_version = df.loc[df['server_name']=='IIS'].groupby(['yr'])['server_version'].agg(lambda x:x.value_counts().index[0])
iis_popular_version = iis_popular_version.reset_index()
iis_popular_version.rename(columns={'server_version': 'iis_popular_version'}, inplace=True)
df = pd.merge(df,iis_popular_version,
    on=['yr'],
    how='left')

# Load the prices of Microsoft software and attach those to the most popular version of IIS in each observation
iis_prices = pd.read_excel("input/iis_prices.xlsx",
    usecols=['server_version','price_2012_standard','price_2012_datacenter'])
iis_prices.rename(columns={
    'server_version': 'iis_popular_version',
    'price_2012_standard': 'iis_pop_p2012s',
    'price_2012_datacenter': 'iis_pop_p2012d'
    },inplace=True)
df = pd.merge(df,iis_prices,
    on=['iis_popular_version'],
    how='left')

# Attached the CPI for each server version number based on when that 
# server version became available.
cpi = pd.read_csv("input/cpi.txt.gz", sep="\t",
    compression='gzip')

cpi.rename(columns={'dm': 'begin_avail_dm'},inplace=True)
o = create_dms(df['begin_avail'])
o.rename(columns={'dt': 'begin_avail', 'dm': 'begin_avail_dm'}, inplace=True)
df = pd.merge(df,o,on=['begin_avail'],how='left')
df = pd.merge(df,cpi,on=['begin_avail_dm'],how='left')

# Add the states from the Orbis data
orbis = pd.read_csv("input/orbis_location.txt.gz", sep="\t", compression='gzip',
    usecols=['domain','state_us'])
orbis.rename(columns={'state_us':'orbis_state', 'fips': 'orbis_fips', 'postcode': 'orbis_postcode'}, inplace=True)
df = pd.merge(df,orbis,on=['domain'],how='left')

# Add the NAICS codes from the Orbis data
orbis = pd.read_csv("input/orbis_naics.txt.gz", sep="\t", compression='gzip',
    usecols=['domain','naics','naicsccod2017'])
orbis.rename(columns={'naics': 'orbis_naics', 'naicsccod2017': 'orbis_naics6'}, inplace=True)
df = pd.merge(df,orbis,on=['domain'],how='left')

# Add the Compustat data on NAICS and state
compustat = pd.read_csv("input/compustat_static.txt.gz", sep="\t", compression='gzip')
compustat.rename(columns={'state':'compustat_state', 'naics': 'compustat_naics', 'naics6': 'compustat_naics6'}, inplace=True)
df = pd.merge(df,compustat,on=['domain'],how='left')


# Combine Orbis and Compustat variables
for variable in ['naics','naics6','state']:
    df[variable] = np.NaN
    df.loc[df[variable].isnull(),variable] = df['compustat_'+variable]
    df.loc[df[variable].isnull(),variable] = df['orbis_'+variable]

# Add the weights to the data to make it representative
df.loc[df['naics']==32,'naics'] = 31
df.loc[df['naics']==33,'naics'] = 31
df.loc[df['naics']==45,'naics'] = 44
df.loc[df['naics']==49,'naics'] = 48

df_n = df.groupby(['state','naics','yr'])['domain'].nunique()
df_n.name = 'sample_n_domains'
df_n = df_n.reset_index()
susb_naics_dfs = pd.read_csv("input/susb_naics_weights.txt",sep="\t")

df = pd.merge(df,susb_naics_dfs,on=['state','naics','yr'],how='left')
df = pd.merge(df,df_n,on=['state','naics','yr'],how='left')


# Filter out firms in Guam and the U.S. Virgin Islands
df = df.loc[~df['state'].isin(['GU',"VI"])]

# Categorical
print("Categorical")
df['domain_id'] = df['domain'].astype('category').cat.codes
df['server_name'] = df['server_name'].astype('category')
df['server_version'] = df['server_version'].astype('category')

# Derived variables
df['susb_weights'] = df['susb_statenaics_firms']/df['sample_n_domains']
df['begin_avail'] = pd.to_datetime(df['begin_avail'])

# Delete any variables from proprietary data
df = df[['domain', 'yr', 'dt',
       'server_name','server_version',
       'interpolated',
       'susb_weights',
       'cpi',
       'iis_popular_version',
       'iis_pop_p2012s','iis_pop_p2012d',
       ]]

# Export the dataset
df.to_stata("output/servers_info_panel.dta",convert_dates={'dt': 'td'})

