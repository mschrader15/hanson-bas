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



# def traditional():
#
#     def func(x):
#         path = x.DeviceId + "\\" + x.Name
#         base_string = f'Print "<{x.Name}> |### </{x.Name}>", {path}'
#         if not isinstance(x.Value, str):
#             base_string = base_string + f' * {int(x.Multiplier)}'
#         return base_string
#
#     for ip in uniqueip:
#         for i, val in enumerate(files['ipaddresses'] == ip):
#             if val:
#                 break
#         # # This writes the unique ip addresses. Not necessary anymore
#         files.loc[i, 'filter_ip'] = "".join([ip, "_trend.xml"])
#         if 'CX1' in files.loc[i, 'DeviceId']:
#             folder = 'CX1'
#         else:
#             folder = 'CX2'
#         # This is generating the unique xml functions:
#         name = files.loc[i, 'Device Tag'] + '.txt'
#         header_lines = ['Print " <Record> "']  # 'Print " <?xml version=|"1.0|"?>"',
#         tail_lines = ['Print " </Record> "']
#         lines = files.loc[files['ipaddresses'] == ip, :].apply(func, axis=1)
#         print_txt = "\n".join(header_lines + list(lines) + tail_lines)
#         with open(os.path.join(definitions.ROOT, 'bas_functions', folder, name), 'w') as f:
#             f.write(print_txt)
#             f.close()


# excel_file_path = os.path.join(definitions.ROOT, 'assets', 'List_AllInputs_new1.xlsx')
#
# files = pd.read_excel(excel_file_path)
# files['filter_ip'] = None
# uniqueip = files['ipaddresses'].unique()


# excel_file_path = os.path.join(definitions.ROOT, 'assets', 'pointListCX2.xlsx')
#
# df = pd.read_excel(excel_file_path)
# # files['filter_ip'] = None
# # uniqueip = files['ipaddresses'].unique()
#
# iterate_df = df.loc[df['USE TRUE/FALSE'] >= 1]
#
# unique_devices = iterate_df['DEVICE'].unique()
#
# def func(x):
#     path = x.DEVICE + "\\" + x.NAME
#     base_string = f'Print "<{x.NAME}> |### </{x.NAME}>", {path}'
#     base_string = base_string + f' * {1000}'
#     return base_string
#
#
# for device in unique_devices:
#     local_df = iterate_df.loc[iterate_df['DEVICE'] == device]
#     name = device.split('\\')[-1]
#     header_lines = ['Print " <Record> "']  # 'Print " <?xml version=|"1.0|"?>"',
#     tail_lines = ['Print " </Record> "']
#     lines = local_df.apply(func, axis=1)
#     print_txt = "\n".join(header_lines + list(lines) + tail_lines)
#     with open(os.path.join(definitions.ROOT, 'bas_functions', 'CX2_new', name + ".txt"), 'w') as f:
#         f.write(print_txt)
#         f.close()


excel_file_path = os.path.join(definitions.ROOT, 'assets', 'pointListCX1.xlsx')

df = pd.read_excel(excel_file_path)
# files['filter_ip'] = None
# uniqueip = files['ipaddresses'].unique()

iterate_df = df.loc[df['Measurement_Kind'] == 'test']

unique_df = iterate_df.groupby('Name').first().reset_index()

def func(x):
    path = x.DeviceId2 + "\\" + x.Name
    base_string = f'Print "<{x.Name}> |### </{x.Name}>", {path}'
    base_string = base_string + f' * {1000}'
    return base_string


header_lines = ['Print " <Record> "']  # 'Print " <?xml version=|"1.0|"?>"',
tail_lines = ['Print " </Record> "']

lines = []
lines = unique_df.apply(func, axis=1).to_list()

print_txt = "\n".join(header_lines + lines + tail_lines)

with open(os.path.join(definitions.ROOT, 'bas_functions', '12_9_test', "test" + ".txt"), 'w') as f:
    f.write(print_txt)
    f.close()



# files.to_excel(excel_file_path)


#
# import requests
#
# url = "https://yoursite/your.xml"
# response = requests.get(url)
# data = xmltodict.parse(response.content)