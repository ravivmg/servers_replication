# The following code converts the unique server header strings into parsed
# information about the server vendor and the version number.
import pandas as pd
from distutils.version import LooseVersion
from utils import *

# Get the unique servers
df = pd.read_csv("output/servers_panel.txt.gz", 
     sep="\t",
     compression='gzip',
     usecols=['server'])
df = df.loc[df['server'].notnull()]
df = df[['server']].drop_duplicates()

# Encode the servers
df['server_name'], df['server_version'] = zip(*df['server'].apply(encode_server_string))
df['version_clean'] = df['server_version'].apply(clean_version)

# We only keep the server version numbers that are for the major server vendors
df_versions = df.loc[(df['server_name'].isin(['Apache','Nginx','IIS','iPlanet'])) & \
     (~df['version_clean'].isnull()) \
     ,['server_name','server_version','version_clean']].drop_duplicates()

# For each version number also attach when that version became available.
df_versions['version_clean'] = df_versions['version_clean'].apply(LooseVersion)
versions_dict = get_version_dict()
df_versions['begin_avail'] = df_versions.apply(find_version_begin_avail, axis=1, args=(versions_dict,))
df_versions['version_clean'] = df_versions['version_clean'].apply(str)

# Attach that data to the output, remembering that we only have information for the versions
# of the major software vendors so make sure to left join
del df['version_clean']
servers_with_date = pd.merge(df,df_versions,
     on=['server_name','server_version'],
     how='left')

# Export the data
servers_with_date.to_csv("output/encoded_servers.txt.gz", 
     sep="\t", compression='gzip', index=False)
