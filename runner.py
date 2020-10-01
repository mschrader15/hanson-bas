import os
import sys
import definitions
import urllib3
import pandas as pd
from functions.logging import logger, logging
from functions.timing import timing
from functions.skyspark import SkySpark
import argparse
from xml.etree import ElementTree
from datetime import datetime, timezone

parser = argparse.ArgumentParser()
parser.add_argument('--offline', dest='offline', action='store_const',
                    const=True, default=False)
parser.add_argument('--multithread', dest='multi_thread', action='store_const',
                    const=True, default=False)
parser.add_argument('--equipment-list', dest='equipment_list', nargs="+")

@timing
def load_master_dict(file_path, filter_list=None):
    df = pd.read_excel(file_path)
    if filter_list:
        filter_list = [item.strip() for item in filter_list]
        df = df.loc[df['Device Tag'].isin(filter_list)]

    unique_ips = df['filter_ip'].dropna().unique()
    device_container = {}
    for ip in unique_ips:
        tag = df.loc[df['filter_ip'] == ip, 'Device Tag'].values[0]
        filtered_df = df.loc[df['Device Tag'] == tag, :]
        d = Device(name=tag, ip_address=ip, measurement_names=list(filtered_df['Name']),
                   units=list(filtered_df['Units']), multipliers=list(filtered_df['Multiplier']),
                   )
        device_container[d.name] = d
    return device_container


def load_offline_xml(file_path):
    f = open(file_path, 'r')
    return f.read()


def get_data(device_obj, xml_as_obj, tag=None):
    print(xml_as_obj)
    root = ElementTree.fromstring(xml_as_obj)
    if tag:
        local_device = device_obj[tag]
    else:
        local_device = device_obj
    for child in root:
        if child.tag in local_device.measurement_names:
            local_device.measurements[child.tag].value = child.text.strip()
            local_device.measurements[child.tag].time = datetime.now()
        child.clear()
    return device_obj


@timing
def fetch_data(master_dict):
    http = urllib3.PoolManager()
    for device in master_dict.values():
        try:
            print("GET request to ", device.ip_address)
            r = http.request('GET', device.ip_address, timeout=urllib3.Timeout(connect=2.0))
            master_dict = get_data(master_dict, xml_as_obj=r.data.decode("utf-8"), tag=device.name)
        # except urllib3.exceptions.MaxRetryError:
        except Exception as e:
            print(device.name, e)
            continue
    return master_dict


def inner_fetch(device, num):
    http = urllib3.PoolManager()
    try:
        print("GET request to ", device.ip_address)
        r = http.request('GET', device.ip_address, timeout=urllib3.Timeout(connect=2.0))
        device = get_data(device, xml_as_obj=r.data.decode("utf-8"))
        # except urllib3.exceptions.MaxRetryError:
    except Exception as e:
        print(device.name, e)
    return num, device


@timing
def fetch_data_multi_threaded(master_dict):
    from concurrent.futures import ThreadPoolExecutor, as_completed	
    # See:  https://creativedata.stream/multi-threading-api-requests-in-python/		
    # it is possible that I will need to wrap this in a timeout function due threading hang-up on windows...
    threads = []
    with ThreadPoolExecutor(max_workers=len(master_dict.values())) as executor:
        for num, device in enumerate(master_dict.values()):
            threads.append(executor.submit(inner_fetch, device, num))
        results = sorted([res for res in [task.result() for task in as_completed(threads)]], key=lambda x: x[0])
        for num, device in enumerate(master_dict.values()):
            device = results[num][1]
    return master_dict


def create_save_df(master_dict, tag=None):
    write_df = pd.DataFrame(index=['value', 'time'])
    if tag:
        local_obj = master_dict[tag]
        for measurement in local_obj.measurements.values():
            write_df.loc['value', measurement.name] = measurement.value
            write_df.loc['time', measurement.name] = measurement.time
    else:
        for local_obj in master_dict.values():
            for measurement in local_obj.measurements.values():
                write_df.loc['value', "_".join([local_obj.name, measurement.name])] = measurement.value
                write_df.loc['time', "_".join([local_obj.name, measurement.name])] = measurement.time
    return write_df.transpose()


def write_to_skyspark(skyspark_obj, master_dict, write_restriction=None):
    for local_obj in master_dict.values():
        for measurement in local_obj.measurements.values():
            if write_restriction:
                if measurement not in write_restriction:
                    continue
            skyspark_obj.write_point_val(equip_name=local_obj.name, point_name=measurement.skyspark_name,
                                         time=measurement.time, value=measurement.value)


def handle_multiplier(master_dict):
    for device in master_dict.values():
        for measurement in device.measurements.values():
            if (measurement.multiplier != 1) and measurement.value:
                measurement.value = float(measurement.value) / measurement.multiplier
    return master_dict


class Device:
    def __init__(self, name, ip_address, measurement_names, units, multipliers):
        self.name = name
        self.ip_address = ip_address
        self.measurements = {vals[0]: Measurement(name, vals[0], vals[1], vals[2])
                             for vals in zip(measurement_names, units, multipliers)}
        self.measurement_names = measurement_names


class Measurement:
    def __init__(self, device_name, name, units, multipliers):
        self.name = name
        self.value = None
        self.time = None
        self.units = units
        self.multiplier = multipliers
        self.skyspark_name = "_".join([device_name, name])


if __name__ == "__main__":

    args = parser.parse_args()
    try:
        container, _ = load_master_dict(definitions.MASTER_TABLE, filter_list=args.equipment_list)
        if args.offline:
            xml_file = load_offline_xml(os.path.join(definitions.ROOT, 'data',
                                                     'MultipleEquip_dataexport_test20200925_1PM.txt'))
            container = get_data(container, xml_file, 'RTU1')
            container = handle_multiplier(container)
            create_save_df(container, tag='RTU1').to_csv(os.path.join(definitions.ROOT, 'data', 'parsed_data.csv'))
        else:
            skyspark = SkySpark(definitions.LOGIN_DICT['SkySpark'])
            if args.multi_thread:
                container, _ = fetch_data_multi_threaded(container)
            else:
                container, _ = fetch_data(container)
            container = handle_multiplier(container)
            write_to_skyspark(skyspark_obj=skyspark, master_dict=container)
            create_save_df(container).to_csv(os.path.join(definitions.ROOT, 'data', 'full_parsed_data.csv'))
    except Exception as e:
        logger.exception("Exception occurred")
        logging.exception("Exception occurred")
        sys.exit(1)
