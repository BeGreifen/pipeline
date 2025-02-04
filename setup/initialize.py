from setup import config_setup
from setup import logging_setup
import os
import configparser
import logging

# Sets global variables like logger and config which can then be imported into other modules
# This helps to keep the same logger instance in all functions across the project.
# All config parameters in [DIRECTORIES] and [FILES] are changed to absolute paths


# Get production configuration
config: configparser.ConfigParser = config_setup.get_prod_config()

# set the project name dynamically from base dir
project_name = os.path.basename(os.path.dirname(__file__))

# Construct the log file path
log_directory = config.get("DIRECTORIES", "logs")
logfile_name = os.path.join(log_directory, f'{os.getlogin()}_logfile.log')

# Initialize logger
logger = logging_setup.init_logger(
    logger_name=project_name,
    logfile_name=logfile_name,
    file_level=logging.INFO,
    console_level=logging.INFO,
    mail_handler=False,
    error_mail_subject=f"[{project_name}]",
    error_mail_recipient=config.get('MAIN', 'error_mail_recipient')
)

PROJECT_NAME = project_name

if __name__ == "__main__":
    logger.info("Logger and configuration setup completed.")
