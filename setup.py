#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import io
from setuptools import setup, find_packages


setup(name='pyspark_xgboost',
      version='0.1.0',
      description='pyspark xgboost',
      keywords='pyspark_xgboost',
      author='Julian Qian',
      author_email='julian.qian@gmail.com',
      url='https://github.com/jq/pyspark_xgboost',
      license='3-clause BSD',
      long_description=io.open(
          './docs/README.rst', 'r', encoding='utf-8').read(),
      platforms='any',
      zip_safe=False,
      # http://pypi.python.org/pypi?%3Aaction=list_classifiers
      classifiers=['Development Status :: 1 - Planning',
                   'Programming Language :: Python',
                   'Programming Language :: Python :: 2',
                   'Programming Language :: Python :: 2.7',
                   'Programming Language :: Python :: 3',
                   'Programming Language :: Python :: 3.3',
                   'Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 3.5',
                   'Programming Language :: Python :: 3.6',
                   ],
      packages=find_packages(exclude=('tests',)),
      include_package_data=True,
      install_requires=[],
      )
