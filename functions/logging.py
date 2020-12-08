import logging
import os
from definitions import LOGIN_DICT, ERROR_LOGS_DIR
from logging.handlers import SMTPHandler

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%d-%b-%y %H:%M:%S',
                    filename=os.path.join(ERROR_LOGS_DIR, 'runner.log'), filemode="a+", )

to_emails = ", ".join([email.strip() for email in LOGIN_DICT['Logging']['to_email'].split(',')])

mail_handler = SMTPHandler(
    mailhost=(LOGIN_DICT['Logging']['mail_server'], LOGIN_DICT['Logging']['port']),
    fromaddr=LOGIN_DICT['Logging']['email'],
    toaddrs=to_emails,
    subject='BAS Data Pull Error',
    credentials=(LOGIN_DICT['Logging']['email'], LOGIN_DICT['Logging']['password']),
    secure=())

logger = logging.getLogger(__name__)
logger.addHandler(mail_handler)

