#!/usr/bin/env python

from distutils.core import setup

from setuptools import find_packages

setup(name='DSS',
      version='1.0',
      description='Discrete Scheduler Simulator',
      author='Calin Iorgulescu',
      author_email='calin.iorgulescu@gmail.com',
      packages=find_packages(),
      py_modules=["utils", "symbex", "simulator"],
      install_requires=[
          'enum34>=1.1.6',
          'more-itertools>=2.4.1',
          'numpy>=1.13.0'
      ],
      scripts=['dss.py'],
      classifiers=[
          'Environment :: Console',
          'License :: OSI Approved :: Apache Software License'
      ],
      license="Apache 2.0"
      )
