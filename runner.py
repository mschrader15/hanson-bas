import os
import sys
import definitions
import urllib3
import pandas as pd
from functions.logging import logger, logging
import argparse
from xml.etree import cElementTree
from datetime import datetime, timezone

parser = argparse.ArgumentParser()
parser.add_argument('--offline', dest='offline', action='store_const',
                    const=True, default=False)


def load_master_dict(file_path):
    df = pd.read_excel(file_path)
    unique_ips = df['filter_ip'].dropna().unique()
    device_container = {}
    for ip in unique_ips:
        tag = df.loc[df['filter_ip'] == ip, 'Device Tag'].values[0]
        d = Device(name=tag, ip_address=ip, measurement_names=list(df.loc[df['Device Tag'] == tag, 'Name'].values))
        device_container[d.name] = d
    return device_container


def load_offline_xml(file_path):
    f = open(file_path, 'r')
    return f


def get_data(master_dict, xml_as_obj, tag):
    local_device = master_dict[tag]
    for event, elem in cElementTree.iterparse(xml_as_obj):
        if elem.tag in local_device.measurement_names:
            local_device.measurements[elem.tag].value = elem.text
            local_device.measurements[elem.tag].time = datetime.now(tz=timezone.utc)
        elem.clear()
    return master_dict


def fetch_data(master_dict):
    http = urllib3.PoolManager()
    for device in master_dict:
        r = http.request('GET', device.ip_address)
        master_dict = get_data(master_dict, xml_as_obj=r.content, tag=device.name)
    return master_dict


def create_save_df(master_dict, tag=None):
    write_df = pd.DataFrame(index=['value', 'time'])
    if tag:
        local_obj = master_dict[tag]
        for measurement in local_obj.measurements.values():
            write_df.loc['value', measurement.name] = measurement.value
            write_df.loc['time', measurement.name] = measurement.time
    return write_df


class Device:
    def __init__(self, name, ip_address, measurement_names):
        self.name = name
        self.ip_address = ip_address
        self.measurements = {measurement_name: Measurement(measurement_name) for measurement_name in measurement_names}
        self.measurement_names = measurement_names

class Measurement:
    def __init__(self, name):
        self.name = name
        self.value = None
        self.time = None


if __name__ == "__main__":
    args = parser.parse_args()
    try:

        container = load_master_dict(definitions.MASTER_TABLE)

        if args.offline:
            xml_file = load_offline_xml(os.path.join(definitions.ROOT, 'data', 'RTU1_dataexport-example.xml'))
            container = get_data(container, xml_file, 'RTU1')
            create_save_df(container, tag='RTU1').to_csv(os.path.join(definitions.ROOT, 'data', 'parsed_data.csv'))
        else:
            master_table = fetch_data(container)

    except Exception as e:
        logger.exception("Exception occurred")
        logging.exception("Exception occurred")
        sys.exit(1)
