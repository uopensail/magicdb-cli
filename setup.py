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

with open("README.md", "r") as fd:
    long_description = fd.read()
with open("requirements.txt", "r") as fd:
    requires_list = fd.readlines()
    requires_list = [i.strip() for i in requires_list]

setup(
    name="magicdb_cli",
    version="1.0.21",
    description="magicdb client cmd tool",
    license="License :: AGPL 3",
    author="TimePi",
    author_email="timepi@uopensail.com",
    url="https://github.com/uopensail/magicdb-cli",
    py_modules=["magicdb_cli"],
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
            'magicdbcli = magicdb_cli.magicdbcli:main'
        ]
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: AGPL 3",
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
