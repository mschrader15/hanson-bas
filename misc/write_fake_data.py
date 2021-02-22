import os
import definitions
import pandas as pd
from functions.skyspark import SkySpark

skyspark = SkySpark(definitions.LOGIN_DICT['SkySpark'])

# %%
df = pd.read_excel(os.path.join(definitions.ROOT, 'assets', 'SkySpark_AdditionalDataPoints.xlsx'))

# %%
data_points = list(df.columns.unique())
desired_points = []
for i, point in enumerate(data_points):
    if "Unnamed" not in point and point not in ['Date', 'Time']:
        desired_points.append(point)


# %%
def func(value, desired_point, ):
    skyspark.append_his_frame(equip_name="Test", point_name=desired_point,
                              time=value['Date'], value=value[desired_point])


# %%
time = df['Date']

for data_point in desired_points:
    df.apply(func, axis=1, args=(data_point, ))
    skyspark.submit_his_frame()

