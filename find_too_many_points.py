import os
import sys
import pandas as pd
import numpy as np
import definitions
from functions.skyspark import SkySparkCreator  # import the SkySpark class (we wrote)
from functions.entities.haystack_objects import Device
import csv

def load_master_dict(file_path, filter_list=None):
    """
    loads the master excel file. creates a dict of "Devices" (aka equipment), each device having multiple
    measurements inside.

    :param file_path: The path to the master excel file. Lists the IP addresses and the sensor names
    :param filter_list: A list of equipment names. Comes from the --equipment-list command line option
    :return: a list of Devices
    """
    df = pd.concat([pd.read_excel(file_path, sheet_name='CX1'), pd.read_excel(file_path, sheet_name='CX2')], axis=0)
    df = df.loc[df['USE TRUE/FALSE'] == True]
    df['Device Tag'] = df['Device Tag'].str.lower()
    if filter_list:
        filter_list = [item.strip() for item in filter_list]
        df = df.loc[df['Device Tag'].isin(filter_list)]
    unique_devices = df['Device Tag'].dropna().unique()
    device_container = {}
    for name in unique_devices:
        filtered_df = df.loc[df['Device Tag'] == name, :].fillna(np.nan).replace([np.nan], [None])
        markers = filtered_df['Equip_Markers'].values[0]
        ip = filtered_df['ipaddresses'].values[0]
        d = Device(name=name, ip_address=ip, measurement_names=list(filtered_df['Name']),
                   units=list(filtered_df['Measurement_Unit_HayStack']), multipliers=list(filtered_df['Multiplier']),
                   equip_markers=markers, measurement_markers=list(filtered_df['Measurement_Marker']),
                   measurement_dataTypes=list(filtered_df['Measurement_Kind'])
                   )
        device_container[d.name] = d
    return device_container


def check_points(sky_spark, device_container):
    print_list = ["Equipment Name", "Missing Points"]
    for device in device_container.values():
        points = sky_spark.find_too_many_points(device.name)
        if len(points):
            desired_names = [measurement.name for measurement in device.measurements.values()]
            extra_points = [point for point in points if point not in desired_names]
            if len(extra_points):
                print_list.append(device.name + ' -- ' + ", ".join(extra_points))
            # print(f"The extra points for {device.name} are: {extra_points}")
        else:
            print(f"device {device} doesn't exist in skyspark")
    # with open("issues.txt", 'w') as f:
    #     c = csv.writer(f)
    #     c.writerows(print_list)
    print("\n".join(print_list))

if __name__ == "__main__":

    devices = load_master_dict(definitions.MASTER_TABLE,)  # filter_list=['rtu2s'])

    skyspark_creator = SkySparkCreator(definitions.LOGIN_DICT['SkySpark'])

    check_points(skyspark_creator, devices)
