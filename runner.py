from io import StringIO
import os
import sys
import definitions
import urllib3
import pandas as pd
from functions.logging import logger, logging
from functions.timing import timing
import argparse
from xml.etree import cElementTree
from datetime import datetime, timezone

parser = argparse.ArgumentParser()
parser.add_argument('--offline', dest='offline', action='store_const',
                    const=True, default=False)


@timing
def load_master_dict(file_path):
    df = pd.read_excel(file_path)
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
    return f


def get_data(master_dict, xml_as_obj, tag):
    local_device = master_dict[tag]
    print(xml_as_obj)
    output = StringIO()
    output.write(xml_as_obj.strip())
    for event, elem in cElementTree.iterparse(output):
        if elem.tag in local_device.measurement_names:
            local_device.measurements[elem.tag].value = elem.text
            local_device.measurements[elem.tag].time = datetime.now(tz=timezone.utc)
        elem.clear()
    return master_dict


@timing
def fetch_data(master_dict):
    http = urllib3.PoolManager()
    for device in master_dict.values():
        #try:
            print("GET request to ", device.ip_address)
            r = http.request('GET', device.ip_address, timeout=urllib3.Timeout(connect=2.0))
            master_dict = get_data(master_dict, xml_as_obj=r.data.decode("utf-8"), tag=device.name)
        # except urllib3.exceptions.MaxRetryError:
        #except Exception as e:
            #print(device.name, e)
            #continue

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
        self.measurements = {vals[0]: Measurement(vals[0], vals[1], vals[2])
                             for vals in zip(measurement_names, units, multipliers)}
        self.measurement_names = measurement_names


class Measurement:
    def __init__(self, name, units, multipliers):
        self.name = name
        self.value = None
        self.time = None
        self.units = units
        self.multiplier = multipliers


if __name__ == "__main__":

    args = parser.parse_args()
    try:
        container, _ = load_master_dict(definitions.MASTER_TABLE)
        if args.offline:
            xml_file = load_offline_xml(os.path.join(definitions.ROOT, 'data', 'RTU1_dataexport-example.xml'))
            container = get_data(container, xml_file, 'RTU1')
            container = handle_multiplier(container)
            create_save_df(container, tag='RTU1').to_csv(os.path.join(definitions.ROOT, 'data', 'parsed_data.csv'))
        else:
            container, _ = fetch_data(container)
            container = handle_multiplier(container)
            create_save_df(container).to_csv(os.path.join(definitions.ROOT, 'data', 'full_parsed_data.csv'))
    except Exception as e:
        logger.exception("Exception occurred")
        logging.exception("Exception occurred")
        sys.exit(1)
