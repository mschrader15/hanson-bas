import logging
import os
from definitions import LOGIN_DICT, ERROR_LOGS_DIR
from logging.handlers import SMTPHandler

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d-%b-%y %H:%M:%S',
                    filename=os.path.join(ERROR_LOGS_DIR, 'runner.error.log'), filemode="a+", )

mail_handler = SMTPHandler(
    mailhost=(LOGIN_DICT['Logging']['mail_server'], LOGIN_DICT['Logging']['port']),
    fromaddr=LOGIN_DICT['Logging']['email'],
    toaddrs=LOGIN_DICT['Logging']['to_email'],
    subject='BAS Data Pull Error',
    credentials=(LOGIN_DICT['Logging']['email'], os.environ.get('LOGGING_PASSWORD')),
    secure=())
logger = logging.getLogger(__name__)
logger.addHandler(mail_handler)

