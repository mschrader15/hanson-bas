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

excel_file_path = os.path.join(definitions.ROOT, 'assets','List_AllInputs.xlsx')

files = pd.read_excel(excel_file_path)
files['filter_ip'] = None
uniqueip = files['ipaddresses'].unique()

def func(x):
    path = x.DeviceId + "\\" + x.Name
    base_string = f'Print "<{x.Name}> |### </{x.Name}>", {path}'
    if not isinstance(x.Value, str):
        base_string = base_string + f' * {int(x.Multiplier)}'
    return base_string

for ip in uniqueip:

    for i, val in enumerate(files['ipaddresses'] == ip):
        if val:
            break
    # # This writes the unique ip addresses. Not necessary anymore
    files.loc[i, 'filter_ip'] = "".join([ip, "_trend.xml"])
    if 'CX1' in files.loc[i, 'DeviceId']:
        folder = 'CX1'
    else:
        folder = 'CX2'
    # This is generating the unique xml functions:
    name = files.loc[i, 'Device Tag'] + '.txt'
    header_lines = ['Print " <Record> "']  # 'Print " <?xml version=|"1.0|"?>"',
    tail_lines = ['Print " </Record> "']
    lines = files.loc[files['ipaddresses'] == ip, :].apply(func, axis=1)
    print_txt = "\n".join(header_lines + list(lines) + tail_lines)
    with open(os.path.join(definitions.ROOT, 'bas_functions', folder, name), 'w') as f:
        f.write(print_txt)
        f.close()

files.to_excel(excel_file_path)


#
# import requests
#
# url = "https://yoursite/your.xml"
# response = requests.get(url)
# data = xmltodict.parse(response.content)