#!/usr/bin/env python

import os, sys 

from setuptools import find_packages 
_packages = find_packages() 

from os.path import join, dirname
# from setuptools import setup
from distutils.core import setup 
from setuptools.extension import Extension 

ext_modules = [
    Extension(
        '_dboxslab',
        sources=[ 
            "../c++/CacheClient.cpp", 
            "./dboxslab_swig.cpp", 
            "./dboxslab_wrap.cxx"
        ],
        
        libraries=[ "boost_system" ],
        
        extra_link_args=[ '/usr/lib64/libdboxcore.a' ], #
        extra_compile_args=["-std=c++11", "-fPIC", '-g0', '-O2', '-I/usr/include/databox', "-D__NOLOGGER__" , ],  
        # '-Wall', "-fPIC", "-std=c++11", '-g0', '-O3', '-Wno-cpp', "-D__DEBUG__", "-D__TRACE__"  
    ) 
] 

setup (name='dboxslab',
       version='1.0',
       author="SWIG Docs",
       description="""Simple dboxslab from docs""",
       ext_modules=ext_modules,
       py_modules=["dboxslab"],
       packages=_packages 
)
