import os
import definitions
import pandas as pd
import pytz
from functions.skyspark import SkySpark

skyspark = SkySpark(definitions.LOGIN_DICT['SkySpark'])

# %%
df = pd.read_excel(os.path.join(definitions.ROOT, 'assets', 'SkySpark_AdditionalDataPointsforPresentation.xlsx'))

# %%
data_points = list(df.columns.unique())
desired_points = []
desired_equip = []
for i, point in enumerate(data_points):
    if "Unnamed" not in point and point not in ['Date', 'Time']:
        equip, point_ = point.split(', ')
        desired_points.append(point_)
        desired_equip.append(equip)


# %%
def func(value, desired_point, _equip):
    skyspark._add_his_value(value['Date'], skyspark._data_type_handler(value[", ".join([_equip, desired_point])]))

df['Date'] = df['Date'].dt.tz_localize(tz='America/Chicago')
df = df.iloc[:-2]
# %%

for data_point, equip in zip(desired_points[2:], desired_equip[2:]):
    print(data_point, equip)
    skyspark._check_equip(equip)
    skyspark._set_point(data_point.split(" ")[-1])
    skyspark._create_his_frame()
    df.apply(func, axis=1, args=(data_point, equip))
    print("submitting ", data_point)
    skyspark.submit_his_frame()

