# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


version = '0.1.0'

with open('README.rst', 'r', encoding='utf-8') as readme_file:
    readme = readme_file.read()

# requirements = [
#     'click>=6.7',
#     'redis-py>=2.10.5',
#     'impyla>=0.14.0',
#     'confluent-kafka>=0.11.0',
#     'protobuf>=3.4.0',
# ]

setup(
    # Application name:
    name='evcard',

    # Version number (initial):
    version=version,

    description='Sample package for Python-Guide.org',
    long_description=readme,

    # Application author details:
    author='duduba',
    author_email='ly.deng@gmail.com',

    # Details
    url='https://github.com/iduduba/samplemod',
    license='BSD',

    # Packages
    packages=find_packages(exclude=('tests', 'docs')),

    # Include additional files into the package
    include_package_data=True,

    # package_data={
    #     # If any package contains *.txt or *.rst files, include them:
    #     # '': ['*.txt', '*.rst'],
    #     'app': ['logging.yaml'],
    # }
)
