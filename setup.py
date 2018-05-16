#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from cml_pipelines import __version__

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('CHANGELOG.rst') as history_file:
    history = history_file.read()

setup(
    name='cml_pipelines',
    version=__version__,
    description="Utilities for constructing pipelines with dask",
    long_description=readme + '\n\n' + history,
    author="Penn Computational Memory Lab",
    url='https://github.com/pennmem/cml_pipelines',
    packages=find_packages(include=['cml_pipelines']),
    include_package_data=True,
    zip_safe=False,
    keywords='pipelines',
)
