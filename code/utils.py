import pandas as pd
from sqlalchemy import create_engine
import re
import tldextract
import json
import numpy as np
from distutils.version import LooseVersion
from dateutil.relativedelta import relativedelta
import datetime

######################################
# Constants
######################################
RE_APACHE = re.compile('(?:Apache(?:$|/([\d.]+)|[^/-])|(?:^|\b)HTTPD)')
RE_NGINX = re.compile('nginx(?:/([\d.]+))?')
RE_ISS = re.compile('IIS(?:/([\d.]+))?')
RE_IPLANET = re.compile('(?:Netscape-Enterprise|Sun-ONE-Web-Server)(?:/([\d.]+( SP[0-9]+)?))?')

URL_CLEANERS = {
    re.compile('^http(s)?:\/\/(http\/\/)?'): '',
    re.compile('^http(s)?:\/\/[0-9]\.'): '',
    re.compile('^www[0-9]*\.'): '',
    re.compile('^ww[0-9]*\.'): '',
    re.compile('\.com(:80)?\/.*$'): '.com',
    re.compile('\.net(:80)?\/.*$'): '.com',
    re.compile('\.org(:80)?\/.*$'): '.org'
}

STATE_DICT = {
        'AK': 'Alaska',
        'AL': 'Alabama',
        'AR': 'Arkansas',
        'AS': 'American Samoa',
        'AZ': 'Arizona',
        'CA': 'California',
        'CO': 'Colorado',
        'CT': 'Connecticut',
        'DC': 'District of Columbia',
        'DE': 'Delaware',
        'FL': 'Florida',
        'GA': 'Georgia',
        'GU': 'Guam',
        'HI': 'Hawaii',
        'IA': 'Iowa',
        'ID': 'Idaho',
        'IL': 'Illinois',
        'IN': 'Indiana',
        'KS': 'Kansas',
        'KY': 'Kentucky',
        'LA': 'Louisiana',
        'MA': 'Massachusetts',
        'MD': 'Maryland',
        'ME': 'Maine',
        'MI': 'Michigan',
        'MN': 'Minnesota',
        'MO': 'Missouri',
        'MP': 'Northern Mariana Islands',
        'MS': 'Mississippi',
        'MT': 'Montana',
        'NA': 'National',
        'NC': 'North Carolina',
        'ND': 'North Dakota',
        'NE': 'Nebraska',
        'NH': 'New Hampshire',
        'NJ': 'New Jersey',
        'NM': 'New Mexico',
        'NV': 'Nevada',
        'NY': 'New York',
        'OH': 'Ohio',
        'OK': 'Oklahoma',
        'OR': 'Oregon',
        'PA': 'Pennsylvania',
        'PR': 'Puerto Rico',
        'RI': 'Rhode Island',
        'SC': 'South Carolina',
        'SD': 'South Dakota',
        'TN': 'Tennessee',
        'TX': 'Texas',
        'UT': 'Utah',
        'VA': 'Virginia',
        'VI': 'Virgin Islands',
        'VT': 'Vermont',
        'WA': 'Washington',
        'WI': 'Wisconsin',
        'WV': 'West Virginia',
        'WY': 'Wyoming'
}

######################################
# Misc
######################################
def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

def clean_version(version):
    if version is None or version=="" or pd.isnull(version):
        return np.NaN
    if isinstance(version, str):
        version = version.rstrip('.')
        if '.' not in version:
            version+=".0"
    try:
        v = LooseVersion(version)
        return str(v)
    except ValueError:
        v = np.NaN

def extract_domain(url):
    o = url
    for k,v in URL_CLEANERS.items():
        o=k.sub(v,o)
    o = tldextract.extract(o)
    o = "%s.%s" % (o.domain,o.suffix)
    # regular expressions to match and replace to clean url
    o=o.split('@')[-1].split(':80')[0].replace('%ef%bb%bf','').replace('%c2%ad','').replace('http://.','').replace('http://www3.','').replace('http://www2.','').replace('https://www.','').replace('http://www.','').replace('https://','').replace('http//','').rstrip('/').lstrip('.')
    o=o.rstrip('.')
    return o


######################################
# Versions
######################################
def get_version_df():
    versions = pd.read_excel("input/versions_with_dates.xlsx")
    versions['begin_avail'] = pd.to_datetime(versions['begin_avail'])
    versions = versions.sort_values("begin_avail")
    versions = versions.loc[versions['version'].notnull()]
    versions['version'] = versions['version'].astype(str)
    versions['version'] = versions['version'].apply(clean_version)
    del versions['sites']
    del versions['source']
    lpad_zero_versions = versions.loc[
        (versions['version'].notnull()) &
        (versions['version'].str.startswith("0."))
        ]
    lpad_zero_versions['version'] = lpad_zero_versions['version'].str.lstrip("0.")
    versions = pd.concat([versions,lpad_zero_versions],axis=0)

    changelog_apache = pd.read_csv("input/apache_begin_avails.txt",sep="\t")
    changelog_apache['name'] = 'Apache'
    changelog_apache['begin_avail'] = pd.to_datetime(changelog_apache['begin_avail'])
    changelog_nginx = pd.read_csv("input/nginx_begin_avails.txt",sep="\t")
    changelog_nginx['name'] = 'Nginx'
    changelog_nginx['begin_avail'] = pd.to_datetime(changelog_nginx['begin_avail'])

    versions = pd.concat([
        versions,
        changelog_nginx,
        changelog_apache
    ],axis=0)
    versions = versions.groupby(['name','version'])['begin_avail'].min()
    versions = versions.reset_index()

    return versions

def get_version_dict():
    versions = get_version_df()
    versions_dict = {}
    for row in versions.iterrows():
        if row[1]['name'] not in versions_dict:
            versions_dict[row[1]['name']] = {}
        versions_dict[row[1]['name']][str(row[1]['version'])]=row[1]['begin_avail']
    return versions_dict

def find_version_begin_avail(row,versions_dict):
    if row['server_name'] in versions_dict:
        return versions_dict.get(row['server_name']).get(str(row['version_clean']))
    return np.NaN


def get_lastest_versions_avail():
    versions = get_version_df()
    versions = versions.sort_values(['begin_avail'])
    # Setup all of the months
    months = []
    end = datetime.datetime(2019, 1, 1)
    current = datetime.datetime(2000, 1, 1)
    while current <= end:
        months.append(current)
        current += relativedelta(months=1)
    # Find the lastest version for each
    df = []
    for month in months:
    	output = {'month': month}
    	avail_versions = versions.loc[(versions['begin_avail']<=month)]
    	for server_vendor in ("Apache","IIS","iPlanet","Nginx"):
    		vendor_avail = avail_versions.loc[avail_versions['name']==server_vendor]
    		if len(vendor_avail)>0:
    			last_vendor_version_avail = vendor_avail.iloc[-1]['begin_avail']
    			output[server_vendor] = diff_month(month,last_vendor_version_avail)
    			output["latest_" + server_vendor.lower() + "_begin_avail_dm"] = last_vendor_version_avail.strftime("%Y-%m")
    	df.append(output)

    df = pd.DataFrame(df)
    df.rename(columns={
        "Apache": 'latest_apache',
        "IIS": 'latest_iis',
        "iPlanet": 'latest_iplanet',
        "Nginx": 'latest_nginx'
        }, inplace=True)
    df['month'] = pd.to_datetime(df['month'])
    df['dm'] = df['month'].dt.strftime("%Y%m")
    df['dm'] = pd.to_numeric(df['dm'])
    del df['month']
    return df

def get_closest_iis():
    versions = get_version_df()
    versions = versions.sort_values(['begin_avail'])
    iis_versions = versions.loc[versions['name']=='IIS']
    # Setup all of the months
    months = []
    end = datetime.datetime(2019, 1, 1)
    current = datetime.datetime(2000, 1, 1)
    while (current <= end):
        months.append(current)
        current += relativedelta(months=1)
    df = []
    for month in months:
        output = {
            'month': month
            }
        tmp = iis_versions.iloc[(iis_versions['begin_avail']-month).abs().argsort()[:1]]
        if len(tmp)>0:
            output['iis_nearest_version'] = tmp.iloc[0]['version']
            df.append(output)
    df = pd.DataFrame(df)
    df['month'] = pd.to_datetime(df['month'])
    df['dm'] = df['month'].dt.strftime("%Y%m")
    df['dm'] = pd.to_numeric(df['dm'])
    del df['month']
    return df

######################################
# Encoding Cookies
######################################
def encode_server_string(server):
    output = {
        'name': "Other",
        'version': None
    }
    if isinstance(server,str):
        found = False
        server = server.strip()
        if found==False:
            m = RE_APACHE.search(server)
            if m:
                output['version'] = m.group(1)
                output['name'] = "Apache"
                found = True
        if found==False:
            m = RE_NGINX.search(server)
            if m:
                output['version'] = m.group(1)
                output['name'] = "Nginx"
                found = True
        if found==False:
            m = RE_ISS.search(server)
            if m:
                output['version'] = m.group(1)
                output['name'] = "IIS"
                found = True
        if found==False:
            m = RE_IPLANET.search(server)
            if m:
                output['version'] = m.group(1)
                output['name'] = "iPlanet"
                found = True
        if found==False:
            if 'mod_ssl' in server or 'IBM_HTTP_Server' in server:
                output['name'] = "Apache"
                found = True
        if found==False:
            if 'Nginx' in server:
                output['name'] = "Nginx"
                found = True
    return output['name'],output['version']

######################################
# DMs
######################################
def create_dms(x):
    x = x.drop_duplicates()
    x = pd.to_datetime(x)
    m = x.dt.strftime("%Y-%m")
    o = pd.concat([x,m],axis=1)
    o.columns = ['dt','dm']
    o = o.loc[o['dt'].notnull()]
    return o