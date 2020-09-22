import pandas as pd
import numpy as np
import definitions
import os
import urllib3

#Below are the formats to automatical generate the necessary scripts.

#This is the ip address format:
#http://10.1.13.71/pe/trendexports.xml

#This is the function call in teh Plain English IDE for Continuum BAS:
#Print " <?xml version=|"1.0|"?>"
#Print " <Record>"
#Print " <temp1> |### </temp1>", Hanson\CX1\VAV1.1

#ip_address = http://10.1.13.71/pe/

excel_file_path = os.path.join(definitions.ROOT, 'List_AllInputs.xlsx')

files = pd.read_excel(excel_file_path)
files['filter_ip'] = None
uniqueip = files['ipaddresses'].unique()

for ip in uniqueip:

    for i, val in enumerate(files['ipaddresses'] == ip):
        if val:
            break

    files.loc[i, 'filter_ip'] = "".join([ip, "_trend.xml"])


    # This is generating the unique xml functions:

    lines = ['Print " <?xml version=|"1.0|"?>"', '<>']
    files.loc[files['ipaddresses'] == ip, :].apply(lambda x:  'Print " ')







files.to_excel(excel_file_path)


#
# import requests
#
# url = "https://yoursite/your.xml"
# response = requests.get(url)
# data = xmltodict.parse(response.content)