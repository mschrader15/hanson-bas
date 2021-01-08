import os
import sys
import pandas as pd
import numpy as np
import definitions
from functions.skyspark import SkySparkCreator  # import the SkySpark class (we wrote)
from functions.entities.haystack_objects import Device


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


def add_equipment(sky_spark, device_container):
    for device in device_container.values():
        sky_spark.add_equipment(device.name, markers=device.get_markers())
        for measurement in device.measurements.values():
            sky_spark.add_measurement(equip_name=device.name, measurement_name=measurement.name,
                                      markers=measurement.get_markers(), unit=measurement.units,
                                      dataType=measurement.dataType, overwrite=True)
#
# def infer_type(device_name):
#     list_of_types = []


if __name__ == "__main__":

    devices = load_master_dict(definitions.MASTER_TABLE,)  # filter_list=['rtu2s'])

    skyspark_creator = SkySparkCreator(definitions.LOGIN_DICT['SkySpark'])

    add_equipment(skyspark_creator, devices)
