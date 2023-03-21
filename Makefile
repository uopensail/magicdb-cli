CURDIR:=$(shell pwd)

OS:=$(shell uname | tr '[A-Z]' '[a-z]')
ARCH:=$(shell uname | tr '[A-Z]' '[a-z]')
.PHONY: build clean


all: build

clean:
	rm -rf build
	rm -rf dist
	rm -rf magicdb.egg-info/

build_whl:
    python3 setup.py bdist_wheel

install:
    python3 setup.py install --install-scripts=/usr/local/bin