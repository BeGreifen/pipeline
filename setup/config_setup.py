import configparser
from os import path

# If you are working with several branches(e.g. master and development)
# it might be useful to have several .ini files.
# You can change between them by call in the respective function (e.g. config = config_setup.get_prod_config())


def get_prod_config() -> configparser.ConfigParser:
    """
    reads a config.ini file from the same directory as this file
    :return:
    """
    return get_config(path.join(path.dirname(__file__), '../config.ini'))


def get_config(config_name: str) -> configparser.ConfigParser:
    """
    reads a config from the path specified in config_name
    :param config_name:
    :return:
    """
    config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    config.read(config_name)
    return config


if __name__ == '__main__':
    print('You called the config_setup module directly. \n \
    I will read the config.ini \n \
    section SECTION_NAME\n \
    the content of constant: example_string_variable \n ')
    conf = get_config('config.ini')
    print(conf.get('SECTION_NAME', 'example_string_variable'))