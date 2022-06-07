#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Setup script.

Uses setuptools.
Long description is a concatenation of README.rst and CHANGELOG.rst.
"""

from setuptools import find_packages, setup

# TODO: is it canonical?

requirements = [l.strip() for l in open('requirements.txt').readlines()]

setup(
    name='pyspookystuff-uav',
    version='0.5.dev0',
    # url='${source_url}',
    # author='${author}',
    # author_email='${author_email}',
    # description='${description}',
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
)
