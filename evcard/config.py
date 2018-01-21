# -*- coding: utf-8 -*-

"""Global configuration handling."""

import copy
import logging
import os
import io

import yaml

from .exceptions import ConfigDoesNotExistException
from .exceptions import InvalidConfiguration


logger = logging.getLogger(__name__)

USER_CONFIG_PATH = os.path.expanduser('~/.evcardrc')

DEFAULT_CONFIG = {
    'impala': {
        'host': '10.20.140.11',
        'port': 21050,
        'database': 'kudu_db'
    },
    'redis': {
        'host': 'localhost',
        'port': 6379,
        'decode_responses': True
    },
    'kafka': {
        'servers': ['evcard-hl-hadoop-04:9092', 'evcard-hl-hadoop-06:9092'],
        'topic': 'ALI_RDS_PCLOUD_PROD',
        'group': 'EVCARD_LOCAL_RDS_SYN_TEST'
    },
    'batch_size': 100,
    'leave_threshold': 6,
}

user_config = None


def _merge_configs(default, overwrite):
    """Recursively update a dict with the key/value pair of another.
    Dict values that are dictionaries themselves will be updated, whilst
    preserving existing keys.
    """
    new_config = copy.deepcopy(default)

    for k, v in overwrite.items():
        # Make sure to preserve existing items in
        # nested dicts, for example `abbreviations`
        if isinstance(v, dict):
            new_config[k] = _merge_configs(default[k], v)
        else:
            new_config[k] = v

    return new_config


def _get_config(config_path):
    """Retrieve the config from the specified path, returning a config dict."""
    if not os.path.exists(config_path):
        raise ConfigDoesNotExistException

    logger.debug('config_path is {0}'.format(config_path))

    with io.open(config_path, encoding='utf-8') as file_handle:
        try:
            yaml_dict = yaml.load(file_handle)
        except yaml.YAMLError as e:
            raise InvalidConfiguration(
                'Unable to parse YAML file {}. Error: {}'
                ''.format(config_path, e)
            )

    # config_dict = _merge_configs(DEFAULT_CONFIG, yaml_dict)

    return yaml_dict


def get_user_config(config_file=None, default_config=False):
    """Return the user config as a dict.
    If ``default_config`` is True, ignore ``config_file`` and return default
    values for the config parameters.
    If a path to a ``config_file`` is given, that is different from the default
    location, load the user config from that.
    Otherwise look up the config file path in the ``EVCARD_CONFIG``
    environment variable. If set, load the config from this path. This will
    raise an error if the specified path is not valid.
    If the environment variable is not set, try the default config file path
    before falling back to the default config values.
    """

    global user_config

    if user_config:
        return user_config

    # Do NOT load a config. Return defaults instead.
    if default_config:
        user_config = copy.copy(DEFAULT_CONFIG)
        return user_config

    # Load the given config file
    if config_file and config_file is not USER_CONFIG_PATH:
        user_config = _get_config(config_file)
        return user_config

    try:
        # Does the user set up a config environment variable?
        env_config_file = os.environ['EVCARD_CONFIG']
    except KeyError:
        # Load an optional user config if it exists
        # otherwise return the defaults
        if os.path.exists(USER_CONFIG_PATH):
            user_config = _get_config(USER_CONFIG_PATH)
        else:
            user_config = copy.copy(DEFAULT_CONFIG)
        return user_config
    else:
        # There is a config environment variable. Try to load it.
        # Do not check for existence, so invalid file paths raise an error.
        user_config = _get_config(env_config_file)
        return user_config


def get_config():
    return user_config
