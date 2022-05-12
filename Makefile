output/servers_panel.txt.gz: code/extract_cookie_data.py
	python code/extract_cookie_data.py

output/encoded_servers.txt.gz: output/servers_panel.txt.gz code/encode_servers.py
	python code/encode_servers.py

output/encoded_dates.txt.gz: output/servers_panel.txt.gz code/encode_dates.py
	python code/encode_dates.py

output/servers_panel_semibalanced.h5: output/servers_panel.txt.gz output/encoded_dates.txt.gz output/encoded_servers.txt.gz code/balance_the_panel.py
	python code/balance_the_panel.py

output/servers_info_panel.dta: output/servers_panel_semibalanced.h5 code/prepare_analytical_dataset.py
	python code/prepare_analytical_dataset.py

all: output/servers_info_panel.dta