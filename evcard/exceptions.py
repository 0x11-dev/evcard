# -*- coding: utf-8 -*-

"""
evcard.exceptions
-----------------------
All exceptions used in the evcard code base are defined here.
"""


class EvcardException(Exception):
    """
    Base exception class. All Evcard-specific exceptions should subclass
    this class.
    """


class ConfigDoesNotExistException(EvcardException):
    """
    Raised when get_config() is passed a path to a config file, but no file
    is found at that path.
    """


class InvalidConfiguration(EvcardException):
    """
    Raised if the global configuration file is not valid YAML or is
    badly constructed.
    """


class InvalidConfiguration(EvcardException):
    """
    Raised if the global configuration file is not valid YAML or is
    badly constructed.
    """
