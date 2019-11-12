#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Evan T. Phelps
#
# Distributed under terms of the MIT license.
"""
Common statistical operations, functions, and utilities.
"""
##############################################################################
import math

import numpy as np
import scipy.stats
from matplotlib import pyplot as plt


def get_sample_size(alpha, beta, diff_numstds=3, one_tailed=False):
    """Calculate the sample size required to achieve Type I and Type II error
    levels, alpha and beta, for given difference in standardized means.
    Assumes equal-variance normal distributions.
    """
    a = alpha if one_tailed else alpha / 2
    z_alpha = scipy.stats.norm.ppf(1 - a)
    z_beta = scipy.stats.norm.ppf(1 - beta)

    return math.ceil(((z_alpha + z_beta) / diff_numstds)**2)


def get_effect_size(std, N, alpha, beta, one_tailed=False):
    """Calculate the raw effect size detectable assuming Type I and II error
    levels of alpha and beta.
    """
    a = alpha if one_tailed else alpha / 2
    z_alpha = scipy.stats.norm.ppf(a)
    z_beta = scipy.stats.norm.ppf(beta)

    return (std * (z_alpha + z_beta)) / math.sqrt(N)


def get_power(std, N, alpha, effect_size, one_tailed=False):
    """Calculate the probability of detecting an effect when the alternative
    hypothesis is true, given N samples, a significance level alpha, and a raw
    effect size effect_size.
    """
    a = alpha if one_tailed else alpha / 2
    z_alpha = scipy.stats.norm.ppf(1 - a)
    se = std / math.sqrt(N)
    crit = z_alpha * se

    return scipy.stats.norm.sf((crit - effect_size) / se)


def binom_test_from_stats(p, N, p0, one_tailed=False):
    """Test of proportion from sample statistic.  For testing sample data
    directly see scipy.stats.binom_test.
    """
    zscore = (p - p0) / math.sqrt(p0 * (1 - p0) / N)
    pscore = scipy.stats.norm.sf(zscore)
    pscore = pscore if one_tailed else 2 * pscore

    return (zscore, pscore)


def zz(X, Y, npoints=10, plot=None):
    lo, hi, stepsize = 0, 100.1, 100 / npoints
    cutoffs = [p for p in np.arange(lo, hi, stepsize) if p <= 100]
    zs_by_quantile = [np.percentile(scipy.stats.zscore(np.sort(x)),
                                    cutoffs) for x in [X, Y]]
    if plot:
        plt.plot(*zs_by_quantile, 'bo')
        x45 = np.linspace(*plt.xlim())
        plt.plot(x45, x45, 'r--')
        plt.grid()
    return zs_by_quantile


def xVnorm(X, plot=None):
    retval = scipy.stats.probplot(X, plot=plot)
    if plot:
        plot.grid()
    return retval
