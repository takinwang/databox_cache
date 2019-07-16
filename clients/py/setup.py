#!/usr/bin/env python

import os, sys 

from setuptools import find_packages 

_packages = find_packages() 

from os.path import join, dirname
from setuptools import setup
from setuptools.extension import Extension

src_path = dirname(os.path.abspath(__file__)) 

setup (name='dboxslab',
       version='1.0',
       author="SWIG Docs",
       description="""Simple dboxslab_rpc from docs""",
#        ext_modules=ext_modules,
       py_modules=[ "__init__", "pysock", "dboxslab_rpc", ],
       packages=_packages 
)
