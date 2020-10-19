import os
import sys
import definitions
import urllib3
import pandas as pd
from functions.logging import logger, logging  # import the logging class (we wrote)
from functions.timing import timing  # import the function timing class (we wrote)
from functions.skyspark import SkySpark  # import the SkySpark class (we wrote)
# from functions.multithread import fetch_data_multi_threaded, \
#     execute_function_multi_threads  # import the multi_thread class
from functions.entities import Device
import argparse
from xml.etree import ElementTree
from datetime import datetime

""" Adding the command line arguments"""
parser = argparse.ArgumentParser()
# used to run the script in offline mode. (pull data from saved xml file. For testing)
parser.add_argument('--offline', dest='offline', action='store_const',
                    const=True, default=False)

# multithread write to SkySpark
parser.add_argument('--multithread', dest='multi_thread', action='store_const', const=True, default=False)

# adding the ability to specify specific equipment to pull & write
parser.add_argument('--equipment-list', dest='equipment_list', nargs="+")


# timing is a wrapper that gives the function time to complete
@timing
def load_master_dict(file_path, filter_list=None):
    """
    loads the master excel file. creates a dict of "Devices" (aka equipment), each device having multiple
    measurements inside.

    :param file_path: The path to the master excel file. Lists the IP addresses and the sensor names
    :param filter_list: A list of equipment names. Comes from the --equipment-list command line option
    :return: a list of Devices
    """
    df = pd.read_excel(file_path)
    if filter_list:
        filter_list = [item.strip() for item in filter_list]
        df = df.loc[df['Device Tag'].isin(filter_list)]

    unique_ips = df['ipaddresses'].dropna().unique()
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
    """
    Loads the offline xml. This is hopefully a depreciated function.

    :param file_path: path to offline xml (emmulates the data returned by the BAS computer)
    :return: file
    """
    f = open(file_path, 'r')
    return f.read()


def process_xml(device_obj, xml_as_obj, tag=None):
    """
    iterates through the passed xml string, adding the values to the corresponding measurement in the passed Device

    :param device_obj: a instance of the Device class OR the device dictionary
    :param xml_as_obj: the xml as a string
    :param tag: if a tag is passed, it is used to index the device dict
    :return: the device object that was passed
    """
    # print(xml_as_obj)
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
    """
    loops through the master device list and pulls the corresponding data from the BAS system

    :param master_dict: the master dict created in load_master_dict
    :return: master_dict
    """
    http = urllib3.PoolManager()
    for device in master_dict.values():
        try:
            print("GET request to ", device.ip_address)
            r = http.request('GET', device.ip_address, timeout=urllib3.Timeout(connect=2.0))
            master_dict = process_xml(master_dict, xml_as_obj=r.data.decode("utf-8"), tag=device.name)
        except Exception as e:
            # catching all exceptions. Should not be the case
            logging.warning(e)
            continue
    return master_dict


def create_save_df(master_dict, tag=None):
    """
    Creates the dataframe to save as a csv. Hopefully not needed

    :param master_dict:
    :param tag:
    :return:
    """
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


@timing
def write_to_skyspark(skyspark_obj, master_dict):
    """
    writes the data to SkySpark using the SkySpark object.

    :param skyspark_obj: instance of the SkySpark class
    :param master_dict: the master dict
    :return: None
    """
    for equipment in master_dict.values():
        print(f"Writing values for {equipment.name}")
        for measurement in equipment.measurements.values():
            skyspark_obj.write_point_val(equip_name=equipment.name, point_name=measurement.skyspark_name,
                                         time=measurement.time, value=measurement.value)


@timing
def write_to_skyspark_frame(skyspark_obj, master_dict):
    """
    Create then write

    :param skyspark_obj: instance of the SkySpark class
    :param master_dict: the master dict
    :return: None
    """
    for equipment in master_dict.values():
        for measurement in equipment.measurements.values():
            skyspark_obj.append_his_frame(equip_name=equipment.name, point_name=measurement.skyspark_name,
                                          time=measurement.time, value=measurement.value)
    result = skyspark_obj.submit_his_frame()
    return result


def handle_multiplier(master_dict):
    """
    This function divides the raw data from the XML by the multiplier that is specified in the master excel file

    :param master_dict: the master dict
    :return: updated master dict
    """
    for device in master_dict.values():
        for measurement in device.measurements.values():
            if (measurement.multiplier != 1) and measurement.value:
                measurement.value = float(measurement.value) / measurement.multiplier
    return master_dict


@timing
def main(args):
    # creating the master dict (called containter here of Devices{Measurements: []}
    container, _ = load_master_dict(definitions.MASTER_TABLE, filter_list=args.equipment_list)

    # if debugging in offline
    if args.offline:
        xml_file = load_offline_xml(os.path.join(definitions.ROOT, 'data',
                                                 'MultipleEquip_dataexport_test20200925_1PM.txt'))
        container = process_xml(container, xml_file, 'RTU1')
        container = handle_multiplier(container)
        create_save_df(container, tag='RTU1').to_csv(os.path.join(definitions.ROOT, 'data', 'parsed_data.csv'))
    else:
        # create the skyspark object
        container, _ = fetch_data(container)
        container = handle_multiplier(container)
        if args.multi_thread:
            # if multithread desired:
            # write_to_skyspark_threaded(master_dict=container, login=definitions.LOGIN_DICT['SkySpark'], threadnum=4)
            print("Threading not supported")
            raise Exception("Threading not supported")
        else:
            skyspark = SkySpark(definitions.LOGIN_DICT['SkySpark'])
            write_to_skyspark_frame(skyspark_obj=skyspark, master_dict=container)

        # create_save_df(container).to_csv(os.path.join(definitions.ROOT, 'data', 'full_parsed_data.csv'))


""" The entry point of the script """
if __name__ == "__main__":

    # parse the command line arguments
    arguments = parser.parse_args()

    # try except clause to catch and log errors
    try:
        main(arguments)

    except Exception as e:
        # log to error log file
        logger.exception("Error occurred")
        # log to the email file when possible
        logging.exception("Error occurred")
        sys.exit(1)
