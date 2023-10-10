#!/usr/bin/python
# -*- coding: UTF-8 -*-
#
# `magicdb-cli` - 'client for magicdb'
# Copyright (C) 2019 - present timepi <timepi123@gmail.com>
# `magicdb-cli` is provided under: GNU Affero General Public License
# (AGPL3.0) https:#www.gnu.org/licenses/agpl-3.0.html unless stated otherwise.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#

# this is setup script for magicdb-cli


from setuptools import find_packages, setup
from os import path
import pathlib

here = pathlib.Path(__file__).parent.resolve()

def get_requires(filename):
    requirements = []
    with open(filename, "rt") as req_file:
        for line in req_file.read().splitlines():
            if not line.strip().startswith("#"):
                requirements.append(line)
    return requirements

def generate_long_description_file():
    this_directory = path.abspath(path.dirname(__file__))
    with open(path.join(this_directory, 'README.md')) as f:
        long_description = f.read()
    return long_description

setup(
    name="magicdb_cli",
    version="1.0.23",
    python_requires='>=3.7',
    description="magicdb client cmd tool",
    license="License :: AGLP3",
    author="TimePi",
    author_email="timepi@uopensail.com",
    url="https://github.com/uopensail/magicdb-cli",
    py_modules=["magicdb_cli"],
    keywords="magicdb client",
    long_description=generate_long_description_file(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    include_package_data=True,
    platforms="any",
    install_requires=get_requires(path.join(here, "requirements.txt")),
    scripts=[],
    entry_points={
        'console_scripts': [
            'magicdbcli = magicdb_cli.magicdbcli:main'
        ]
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU Affero General Public License v3",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
    ],
    zip_safe=False
)
