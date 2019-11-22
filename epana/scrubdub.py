#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Evan T. Phelps
#
# Distributed under terms of the MIT license.
"""
Basic utilities for low-level data cleaning and characterizing.
"""
##############################################################################
import io
import shutil
import string
from collections import Counter

import cchardet as chardet

import ftfy

try:
    basestring
except NameError:
    basestring = (str, bytes)


def isstring(s):
    return isinstance(s, basestring)


def head(fname, N=10):
    with open(fname, 'rb') as fin:
        return [next(fin).rstrip() for i in range(N)]


def guess_encoding(fn):
    """Return a guess of encoding scheme of file fn."""
    guess = None
    with open(fn, 'rb') as f:
        blk = b''.join(f.readlines())
        guess = chardet.detect(blk)['encoding']
    # This should NOT be required, but there seems to be a bug in
    # either chardet or the csv package.
    guess = 'ISO-8859-1' if guess == 'WINDOWS-1252' else guess
    return guess


# TODO: Incorporate this regex version into the active count_chars
#       or consider change charclass counter to match patterns.
# def count_cfreq_prec(fn, patterns):
#     cntrs = {ptrn: Counter() for ptrn in patterns}
#     ptrns_compiled = {ptrn: re.compile(ptrn) for ptrn in patterns}
#     with fopen(fn) as fin:
#         ufin = decrypt(fin) if fn.endswith('.gpg') else fin
#         for rec in ufin:
#             for ptrn, c in cntrs.items():
#                 c[len(ptrns_compiled[ptrn].findall(rec))] += 1
#     return cntrs

def count_chars(fn, chars):
    cntrs = {ch: Counter() for ch in chars}
    with open(fn) as fin:
        for rec in fin:
            for ch, c in cntrs.items():
                c[rec.count(ch.encode())] += 1
    return cntrs


def get_charclass(c):
    """Returns the class of a single character c."""
    ccats = {string.ascii_letters: ':alpha:',
             string.digits: ':digit:',
             '%*/+-^<>=': ':math:',
             '$€£': ':$€£:',
             ',': ',',
             '|': '|',
             '\r': '\r',
             '\n': '\n',
             '\t': '\t',
             ' ': ' ',
             string.punctuation: ':punct:'
             # string.whitespace:':wspace:',
             }
    for k, v in ccats.items():
        if c in k:
            return v
    if len(c) > 1:
        return ':multibyte:'
    return ':unknown:'


def iterable_to_stream(iterable, buffer_size=io.DEFAULT_BUFFER_SIZE):
    """Yields read-only bytestrings.

    Lets you use an iterable (e.g. a generator) that yields bytestrings as a
    read-only input stream.  The stream implements Python 3's newer I/O API
    (available in Python 2's io module).  For efficiency, the stream is
    buffered.

    Credit to Mechanical snail on stackoverflow.com; modified for the current
    special case requiring a byte stream.
    """
    class IterStream(io.RawIOBase):
        def __init__(self):
            self.leftover = None

        def readable(self):
            return True

        def readinto(self, b):
            try:
                lngth = len(b)  # We're supposed to return at most this much
                chunk = self.leftover or next(iterable).encode('utf8')
                output, self.leftover = chunk[:lngth], chunk[lngth:]
                b[:len(output)] = output
                return len(output)
            except StopIteration:
                return 0    # indicate EOF
    return io.BufferedReader(IterStream(), buffer_size=buffer_size)


def tag_chrs(s, cats={string.ascii_letters: 'a',
                      '0': '0',
                      string.digits: '9'}):
    """Returns a string of tags in one-to-one correspondence with the
    characters of the string argument s according to the character categories
    specified in the dict argument cats.

    TODO: Should this use the ccats of get_charclass?
    """
    def mapme(c):
        for k, v in cats.items():
            if c in k:
                return v
        return c
    return ''.join([mapme(c) for c in s])


def chunk_chrs(s):
    """Returns two lists: characters and number of consecutive occurrences.
    """
    s_reduced, s_run_counts = [], []
    c_prev, run_cnt = None, 0
    for c in s:
        if c == c_prev:
            run_cnt += 1
        else:
            s_reduced.append(c)
            s_run_counts.append(run_cnt)
            run_cnt = 1
            c_prev = c
    return (s_reduced, s_run_counts)


def count_charclasses(fn, fix_unicode=False):
    """Returns character class Counter for header and body of file fn"""
    bcounts, bcountsH = None, None

    with open(fn, 'rb') as fin:
        if fix_unicode:
            guess = guess_encoding(fn)
            fin_fixed = ftfy.fix_file(fin, encoding=guess)
            with iterable_to_stream(fin_fixed) as bffr:
                bcountsH = Counter(bffr.readlines(1)[0])
                bcounts = Counter(bffr.read())
        else:
            bcountsH = Counter(fin.readlines(1)[0])
            bcounts = Counter(fin.read())

    ccountsH = {(get_charclass(chr(k)), chr(k), k): n
                for (k, n) in bcountsH.items()}
    ccounts = {(get_charclass(chr(k)), chr(k), k): n
               for (k, n) in bcounts.items()}

    charclassesH = Counter()
    for k, v in ccountsH.items():
        charclassesH[k[0]] += v
    charclasses = Counter()
    for k, v in ccounts.items():
        charclasses[k[0]] += v
    return (charclassesH, charclasses)


def fix_unicode_and_copy(fn_i, fn_o):
    """Fix unicode of file fn_i and copy to fn_o."""
    guess = guess_encoding(fn_i)
    if guess != 'UTF-8':
        with open(fn_o, 'w', encoding='utf8') as fout, open(fn_i, 'rb') as fin:
            for line in ftfy.fix_file(fin, encoding=guess):
                fout.write(line)
    else:
        shutil.copyfile(fn_i, fn_o)
