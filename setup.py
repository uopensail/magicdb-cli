#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: timepi
@description: this is setup script for magicdb-cli
"""

from setuptools import setup, find_packages

with open("README.md", "r") as fd:
    long_description = fd.read()
with open("requirements.txt", "r") as fd:
    requires_list = fd.readlines()
    requires_list = [i.strip() for i in requires_list]

setup(
    name="magicdbcli",
    version="1.0.19",
    description="magicdb client cmd tool",
    license="License :: GPL 3",
    author="TimePi",
    author_email="timepi@uopensail.com",
    url="https://github.com/uopensail/magicdb-cli",
    py_modules=["magicdbcli"],
    keywords="magicdb client",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    include_package_data=True,
    platforms="any",
    install_requires=requires_list,
    scripts=[],
    entry_points={
        'console_scripts': [
            'magicdbcli = magicdbcli.magicdbcli:main'
        ]
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: GPL 3",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
    ],
    zip_safe=False
)
