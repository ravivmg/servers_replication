# The following code takes the raw server data and turns it into a panel
# We do a conservative imputation of missing observations in the dataset.
import pandas as pd
import numpy as np
from utils import *

# Load the raw header data
df = pd.read_csv('output/servers_panel.txt.gz', sep="\t",compression='gzip')

# Clean the dates
dates = pd.read_csv("output/encoded_dates.txt.gz", sep="\t", compression='gzip')
dates['dt'] = pd.to_datetime(dates['dm'],format='%Y%m')
dates = dates.loc[dates['dt'].notnull()]
dates = dates.loc[dates['yr'].notnull()]
dates = dates.loc[(dates['yr']>=2000) & (dates['yr']<=2018)]
df = pd.merge(df,dates,on='date')
del df['date']

# Clean the target urls into domains 
# (some have paths or sufffixes that need to be removed)
domains = df[['target_url']].drop_duplicates()
domains['domain'] = domains['target_url'].apply(extract_domain)
df = pd.merge(df,domains,on='target_url',how='left')

# Add the encoded the server version and vendor
servers = pd.read_csv("output/encoded_servers.txt.gz",
    sep="\t",
    compression='gzip',
    usecols=['server','server_name','server_version'])
df = pd.merge(df,servers,on='server',how='left')
df['server_name'] = df['server_name'].fillna("")
df['server_version'] = df['server_version'].fillna("")


# In the event that we have more than one observation per month then we want to use
# the one observation that has the server vendor and version data
# (i.e. if you have two observations in a month and only one has data then keep the one with data).
# We also want to take the server vendor and version number that is modal in the case where a firm
# used multiple server vendors and versions within a month.
# We do this by flagging the observations
# For observations with server vendor and version, then we use the number of observations of that.
# -1 if the vendor is there but not the version
# -2 if the vendor and the version are missing
# Then we sort ascending and pick the first observation.
obs_per_server = df.groupby(['domain','dt','server_name','server_version']).size()
obs_per_server.name = 'obs'
obs_per_server = obs_per_server.reset_index()

df = pd.merge(df,obs_per_server,on=['domain','dt','server_name','server_version'],how='left')
df.loc[
    (df['server_name'].notnull()) | (df['server_version'].isnull()),
    'obs'
    ] = -1

df.loc[
    (df['server_name'].isnull()) & (df['server_version'].isnull()),
    'obs'
    ] = -2

df = df.sort_values(['domain','dt','obs'],ascending=[True,True,False])
df = df.groupby(['domain','dt']).first()
df = df.reset_index()
del df['obs']

# Impute missing observations.
# The idea here is that if we see a server vendor and version in one month and then again in another with a gap
# we should fill that in and assume that they continued using the same vendor and version.
# But if they change and there is a gap before that then you definitely don't want to do impute those because
# we don't actually know exactly which month they switched.
df = df.sort_values(['domain','dt'])
df = df[['domain','dt','server_name','server_version']]

# Start by making all of the observations where we don't have data be blank strings.
# The reason for this is that we don't want to impute over times when we have a snapshot
# but the snapshot wasn't one of the major server vendors or versions.
# That is still observed data.
for col in ['server_name','server_version']:
    df.loc[
        df[col]=='',
        col
    ] = np.NaN

# Get all of the observations where we actually have data
# and add a flag that these data observations are not interpolated.
# Also going forward, find what the next observation's vendor and version are.
observed_data = df.loc[df['server_name'].notnull()]
for col in ['server_name','server_version']:
    observed_data[col+"_next"] = observed_data.groupby(["domain"])[col].shift(-1)

observed_data['interpolated'] = 0

# For each domain, create a full range of monthly observations between the first time
# we observe them in the data and the last time we observe them.
domains = df[['domain']].drop_duplicates()
domains['key'] = 1
dts = pd.DataFrame(pd.date_range(min(df.dt),max(df.dt),freq='MS'),columns=['dt'])
dts['key'] = 1

# Add to the data information on the first observation and last observation
first_ob = df.groupby("domain")['dt'].min()
first_ob.name = 'dt_first'
first_ob = first_ob.reset_index()

last_ob = df.groupby("domain")['dt'].max()
last_ob.name = 'dt_last'
last_ob = last_ob.reset_index()

# Join the domains with the full balanced set of observations to make each domain balanced.
m = pd.merge(domains,dts,on='key')
m = pd.merge(m,first_ob,on='domain')
m = pd.merge(m,last_ob,on='domain')
m = m.loc[
        (m['dt']>=m['dt_first']) &
        (m['dt']<=m['dt_last'])
    ]
del m['dt_first']
del m['dt_last']

# Attached the observed data to the balanced panel
m = pd.merge(m,observed_data,on=['domain','dt'],how='left')
del m['key']

# Flag any observations that were not interpolated.
m['interpolated'] = m['interpolated'].fillna(1)


m.loc[(m['interpolated']==0) & (m['server_name'].isnull()),'server_name'] = ''
m.loc[(m['interpolated']==0) & (m['server_version'].isnull()),'server_version'] = ''

# This is where the interpolation really happens.
# Start by filling forward the server vendor
# If the observation is an interpolated observation
# and the server vendor that is NOT the same as before
# then do not fill that forward.
m['server_name'] = m.groupby(['domain'])['server_name'].fillna(method='ffill')
m['server_name_next'] = m.groupby(['domain'])['server_name_next'].fillna(method='ffill')
m.loc[
    (m['server_name']!=m['server_name_next']) &
    (m['interpolated']==1)
    ,
    'server_name'
    ] = np.NaN

# Similarly fill forward the server version
# If the observation is an interpolated observation
# and the server version is NOT the same as before
# then do not fill that forward. Also if the server vendor
# changed then you want to not fill that forward too.
m['server_version'] = m.groupby(['domain'])['server_version'].fillna(method='ffill')
m['server_version_next'] = m.groupby(['domain'])['server_version_next'].fillna(method='ffill')
m.loc[
    (
        (m['server_name']!=m['server_name_next']) |
        (m['server_version']!=m['server_version_next'])
    ) &
    (m['interpolated']==1)
    ,
    'server_version'
    ] = np.NaN


# Get rid of any temporary data
# Also turn the situations where we dont have data back to nulls
m = m.loc[m['server_name'].notnull()]
m = m[['domain','dt','server_name','server_version','interpolated']]
m.loc[m['server_name']=='','server_name'] = np.NaN
m.loc[m['server_version']=='','server_version'] = np.NaN

# Export
m.to_hdf('output/servers_panel_semibalanced.h5', 'df')