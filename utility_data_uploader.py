import os
import sys
import definitions
from functions.logging import logger, logging
from functions.skyspark import SkySpark
from functions.entities import Device
from functions.uploader import FlatFile

import argparse

# adding the arguments
parser = argparse.ArgumentParser()
parser.add_argument('--entity-name', dest='equipment_name')
parser.add_argument('--file-path', dest='upload_file_path')
parser.add_argument('--point-list', dest='point_list', nargs="+")
parser.add_argument('--time-column', dest='time_column')


def create_point_gen(point_list_string):
    for item in point_list_string:
        yield item.strip()


def main(args):

    # make the skyspark connection:
    skyspark = SkySpark(definitions.LOGIN_DICT['SkySpark'])

    # get the utility file
    utility_file = FlatFile(args.upload_file_path, args.time_column)

    # create point generator
    point_gen = create_point_gen(args.point_list)

    # loop through the points and write the series
    for point in point_gen:
        series = utility_file.get_his_series(point)
        res = skyspark.submit_his_series(args.equipment_name, "_".join([args.equipment_name, point]), series)
        print(res.result)



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