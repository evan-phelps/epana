#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2017 Evan T. Phelps
#
# Distributed under terms of the MIT license.

from setuptools import find_packages, setup

setup(
    name='epana',
    version='0.0.1',
    license='MIT',
    author='Evan T. Phelps',
    author_email='ephelps@omegaas.com',
    packages=find_packages(),
    install_requires=['scipy',
                      'pandas',
                      'ftfy',
                      'cchardet'
                      ],
    py_modules=['logutils',
                'scrubdub',
                'tabular',
                'crosstabular',
                'cryptic',
                'stats',
                'db'],
    entry_points={}
)
