import logging
import logging.handlers
from pathlib import Path
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import configparser
from setup import config_setup

def get_logger(logger_name, **kwargs):
    """
    Returns the wanted logger. A new one will be created if a logger with logger_name does not exist.
    Otherwise, the existing logger will be returned.
    It is advised to call get_logger if you need logging abilities in different files.

    :param logger_name: str
    :param kwargs: all params from init_logger
    :return: logger instance
    """
    logger_dict = logging.root.manager.loggerDict
    if logger_name in logger_dict:
        logger = logging.getLogger(logger_name)
    else:
        logger = init_logger(logger_name, **kwargs)
    return logger

def init_logger(logger_name='default_logger', logfile_name='log.log', console_level=logging.INFO, file_level=logging.DEBUG,
                mail_handler=False,  mail_level=logging.WARNING,
                smtp_server='', smtp_port=587, smtp_user='', smtp_password='', error_mail_recipient='', error_mail_subject="[Example]") -> logging.Logger:
    Path(os.path.dirname(logfile_name)).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    logger.addHandler(
        get_console_handler(console_level))
    logger.addHandler(
        get_file_handler(logfile_name, file_level))
    if mail_handler:
        error_mail_recipient_list = error_mail_recipient.replace(' ', '').split(',')
        logger.addHandler(
            get_mail_handler(smtp_server, smtp_port, smtp_user, smtp_password, error_mail_recipient_list, error_mail_subject, mail_level))
    return logger

def get_console_handler(level=logging.INFO) -> logging.Handler:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    return console_handler

def get_file_handler(filename, level=logging.DEBUG) -> logging.Handler:
    file_log_handler = logging.handlers.RotatingFileHandler(filename, maxBytes=262144 * 8, backupCount=10)
    file_log_handler.setLevel(level)
    file_log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    return file_log_handler

def get_mail_handler(smtp_server, smtp_port, smtp_user, smtp_password, recipient_list, subject, level=logging.WARNING) -> logging.Handler:
    mail_handler = SMTPHandler(smtp_server, smtp_port, smtp_user, smtp_password, recipient_list, subject)
    mail_handler.setLevel(level)
    mail_handler.setFormatter(logging.Formatter('%(message)s'))
    return mail_handler

class SMTPHandler(logging.Handler):
    def __init__(self, smtp_server, smtp_port, smtp_user, smtp_password, toaddrs, subject):
        logging.Handler.__init__(self)
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        if isinstance(toaddrs, str):
            toaddrs = [toaddrs]
        self.toaddrs = toaddrs
        self.subject = subject

    def emit(self, record):
        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_user
            msg['To'] = ', '.join(self.toaddrs)
            msg['Subject'] = self.subject + " " + record.levelname

            body = self.format(record)
            msg.attach(MIMEText(body, 'plain'))

            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.smtp_user, self.smtp_password)
            text = msg.as_string()
            server.sendmail(self.smtp_user, self.toaddrs, text)
            server.quit()
        except Exception:
            self.handleError(record)

def configure_logs_directory() -> Path:
    """
    Ensures that a logs directory exists and returns its absolute path.

    Returns:
        Path: Absolute path to the logs directory.
    """
    # Retrieve logs directory from your config
    config: configparser.ConfigParser = config_setup.get_prod_config()
    logs_path: Path = Path(config.get("DIRECTORIES", "logs")).resolve()
    logs_path.mkdir(parents=True, exist_ok=True)  # create if non-existent
    return logs_path