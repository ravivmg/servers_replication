import pandas as pd

# Load the raw server header with the raw string data
df = pd.read_csv('output/servers_panel.txt.gz', sep="\t",compression='gzip')

# Find only the unique dates and only the date part of the timestaps
dates = df[['date']].drop_duplicates()
dates['date_short'] = dates['date'].str[4:16]
short_dates = dates[['date_short']].drop_duplicates()

# Convert the string dates into properly formatted dates
short_dates['dt'] = pd.to_datetime(short_dates['date_short'],errors='coerce')
short_dates = short_dates.loc[short_dates['dt'].notnull()]
short_dates = pd.merge(dates,short_dates,on='date_short')

remaining_dates = dates.loc[~dates['date_short'].isin(short_dates['date_short'])]
remaining_dates['dt'] = pd.to_datetime(remaining_dates['date'],errors='coerce')
remaining_dates = remaining_dates.loc[remaining_dates['dt'].notnull()]

# Extract both the date, month, and year
dates = pd.concat([short_dates,remaining_dates],axis=0)
dates['dt'] = pd.to_datetime(dates['dt'],errors='coerce',utc=True)
dates['dm'] = dates['dt'].dt.strftime("%Y%m")
dates['yr'] = dates['dt'].dt.strftime("%Y")
dates['yr'] = pd.to_numeric(dates['yr'],errors='coerce')

# Drop any dates where we don't have a year
dates = dates.loc[dates['yr'].notnull()]
del dates['date_short']

# Export
dates.to_csv("output/encoded_dates.txt.gz", 
    sep="\t", compression='gzip', index=False)
