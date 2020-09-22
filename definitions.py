import os
import configparser

ROOT = os.path.dirname(os.path.abspath(__file__))


def get_login_info(root, login_info_file):
    config = configparser.ConfigParser()
    config.read(os.path.join(root, login_info_file))
    return config


LOGIN_DICT = get_login_info(ROOT, 'login.txt')
MASTER_TABLE = os.path.join(ROOT, 'assets', 'List_AllInputs.xlsx')
ERROR_LOGS_DIR = os.path.join(ROOT, 'errors')