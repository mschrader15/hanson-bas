import pandas as pd
import numpy as np

#Below are the formats to automatical generate the necessary scripts.

#This is the ip address format:
#http://10.1.13.71/pe/trendexports.xml

#This is the function call in teh Plain English IDE for Continuum BAS:
#Print " <?xml version=|"1.0|"?>"
#Print " <Record>"
#Print " <temp1> |### </temp1>", Hanson\CX1\VAV1.1

#ip_address = http://10.1.13.71/pe/

files = pd.read_excel(r'C:\Users\coals01826\Desktop\Coding\workingdata\List_AllInputs.xlsx')
files

uniqueip = np.unique(files['ipaddresses'].values)
uniqueip


