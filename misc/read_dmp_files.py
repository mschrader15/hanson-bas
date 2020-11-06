import pandas as pd
from definitions import ROOT
import os

file_names = ['pointListBCX3.dmp', 'pointListCX2.dmp', 'pointListCX1.dmp']

files = [os.path.join(ROOT, 'assets', name) for name in file_names]

match_template = "'TYPE                   : DEVICE                       : NAME"

for file in files:
    file_length = sum(1 for _ in open(file, 'r'))
    f = open(file, 'r')  # inefficient but finding the length makes me read it in twice.
    # could also set length to something huge (1e9)
    start_line = file_length
    values = []
    header_values = []
    for i, line in enumerate(f):
        if i >= start_line:
            if (line != '\n') and not ('EndDictionary' in line):
                local_dict = {key: val for key, val in zip(header_values, [item.strip() for item in line.split(':')])}
                values.append(local_dict)
                continue
            else:
                start_line = file_length
                continue
        if match_template in line:
            # print(line)
            start_line = i + 1
            header_values = [item.strip() for item in line.split(':')]
    pd.DataFrame.from_records(values).fillna('').to_excel(file.split('.')[0] + '.xlsx')
