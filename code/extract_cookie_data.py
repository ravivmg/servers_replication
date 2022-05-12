# The following file extracts the server name and ip addresses from the raw header data
# provided by the Internet Archive.
import pandas as pd
import json
import numpy as np
import glob
import zipfile

# Initialize an array to store the extracted header data.
output = []

# Go through each of the raw data files
zip_files = glob.glob("input/ia/kenji/archive.org/~kenji/wayback-response-headers/*.zip")
for zip_file in zip_files:
    with zipfile.ZipFile(zip_file, "r") as f:
        for crawl_file in f.namelist():
            # If the raw data file is just errors then skip it.
            if crawl_file.endswith(".err"):
                continue
            data = f.read(crawl_file)
            crawls = data.decode("utf8").strip().split("\n")
            # Go through each of the snapshots within a raw data file
            for crawl in crawls:
                # Some snapshots are empty. Skip those instead of trying to parse.
                if crawl=="":
                    continue
                # Parse to json
                crawl_data = json.loads(crawl)
                # Extract header, date, and the URI that was targeted by the Internet Archive
                headers = crawl_data.get("Envelope",{}).get("Payload-Metadata",{}).get("HTTP-Response-Metadata",{}).get("Headers",{})
                date = crawl_data.get("Envelope",{}).get("Payload-Metadata",{}).get("HTTP-Response-Metadata",{}).get("Headers",{}).get("Date",np.NaN)
                header_metadata = crawl_data.get("Envelope",{}).get("WARC-Header-Metadata",None)
                if header_metadata is None:
                    header_metadata = crawl_data.get("Envelope",{}).get("ARC-Header-Metadata",None)
                server = crawl_data.get("Envelope",{}).get("Payload-Metadata",{}).get("HTTP-Response-Metadata",{}).get("Headers",{}).get("Server",np.NaN)
                target_url = header_metadata.get("Target-URI",np.NaN)
                # Add these to the output as an observation
                output.append({
                    'target_url': target_url,
                    'server': server,
                    'date': date
                })

# Convert the output to a data frame for writing
output = pd.DataFrame(output)

# Load data that we got through the Internet Archive's API
api_data = pd.read_json("input/extract_from_api/outputs/header.jl", lines=True)
api_data.rename(columns={'Date':'date','Server': 'server'}, inplace=True)

# Combine the raw data from the Internet Archive with the API data
output = pd.concat([output,api_data],axis=0)

# Export to a tab delimited file.
output.to_csv("output/servers_panel.txt.gz", 
    sep="\t", index=False,compression='gzip')
