#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8:ts=4;sw=4
#
# Copyright © 2018 Evan T. Phelps
#
# Distributed under terms of the MIT license.
"""
Logger wrapper and associated convencience functions.
"""
import logging
import os
import time
# from functools import wraps

ENV_LOGLEVEL = 'LOGLEVEL'
LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}


def get_logger(logname):
    logging.basicConfig(level=LEVELS.get(os.getenv(ENV_LOGLEVEL),
                                         logging.INFO),
                        format='%(asctime)s.%(msecs)03d | ' +
                               # '%(filename)s:%(lineno)d [%(funcName)s] | ' +
                               '%(levelname)s | %(message)s',
                        datefmt='%s')  # '%Y-%m-%d %H:%M:%S')
    log = logging.getLogger(logname)
    # log.addHandler(logging.StreamHandler())
    return log


def mstime():
    return int(round(time.time() * 1000))


def test_get_logger():
    log = get_logger('test log name')
    log.debug('I am debug.')
    log.info('I am info.')
    log.warn('I am warn.')
    log.error('I am error.')
    log.critical('I am critical.')
    log.setLevel(logging.INFO)
    assert log.getEffectiveLevel() == logging.INFO
    assert not log.isEnabledFor(logging.DEBUG)
    assert log.isEnabledFor(logging.WARN)


def test_mstime():
    wtime_ms = 50
    threshold_ms = 5
    t0 = mstime()
    time.sleep(1.0 * wtime_ms / 1000)
    t1 = mstime()
    dt = t1 - t0 - wtime_ms
    assert dt >= 0 and dt < threshold_ms
