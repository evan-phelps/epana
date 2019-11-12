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
import getpass
import os
from contextlib import contextmanager
from functools import partial
from io import BytesIO, StringIO

import gnupg

import paramiko

try:
    basestring
except NameError:
    basestring = (str, bytes)


def isstring(s):
    return isinstance(s, basestring)


def ssh_ls(path_expr, srvr, usr, pwd,
           known_hosts=os.environ['HOME'] + '/.ssh/known_hosts'):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys(known_hosts)
    ssh.connect(srvr, username=usr, password=pwd)
    try:
        ftp = ssh.open_sftp()
        for fn in ftp.listdir(path_expr):
            yield fn
    finally:
        ssh.close()


@contextmanager
def ssh_open(fpaths, srvr, usr, pwd,
             known_hosts=os.environ['HOME'] + '/.ssh/known_hosts'):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.load_system_host_keys(known_hosts)
    ssh.connect(srvr, username=usr, password=pwd)
    try:
        ftp = ssh.open_sftp()
        fpaths = [fpaths] if isstring(fpaths) else fpaths
        for fpath in fpaths:
            yield ftp.open(fpath, 'rb', bufsize=1048576)
    finally:
        ssh.close()


def decrypt(fin, pwd=None, ostream=False):
    # Some systems might need binary=/usr/bin/gpg2 if multiple versions
    # are installed.  Consider adding parameter to specify.
    gpg = gnupg.GPG()  # homedir='~/.gnupg')
    pwd = getpass.getpass(
        'private key password: ') if pwd is None else pwd
    d = gpg.decrypt_file(fin, passphrase=pwd, always_trust=True)
    if ostream is True:
        try:
            return StringIO(d.data.decode())
        except UnicodeDecodeError as e:
            print(e)
            return StringIO(d.data.decode('ISO-8859-1'))
    else:
        # return d.data.decode('utf-8').rstrip(os.linesep).split(os.linesep)
        return d.data.rstrip(os.linesep.encode()).split(os.linesep.encode())


def head(fname, N=10, bytes=None):
    if fname.endswith('.gpg'):
        s = None
        with fopen(fname) as fin:
            s = fin.read(131076)
        pwd = getpass.getpass('private key password: ')
        return decrypt(BytesIO(s), pwd)[0:N]
    elif bytes is True:
        # return buf.read(N).split(os.linesep)
        with fopen(fname) as buf:
            return buf.read(N)
    else:
        with fopen(fname) as buf:
            # return [next(buf).rstrip(bytes(os.linesep, encoding='utf8'))
            #         for i in range(N)]
            return [next(buf).rstrip() for i in range(N)]


_srvr_pwds = {}  # bad way to avoid re-entering passwords!


@contextmanager
def fopen(fpath):
    # srvr_pwds = {}
    (user, password, server, dirpath) = (None, None, None, None)
    this_open = partial(open, mode='rb')
    cred_path_parts = fpath.split('@')
    if len(cred_path_parts) > 2:
        raise Exception('Unrecognized URL format, too many \'@\' symbols')
    if len(cred_path_parts) == 2:
        cred_part = cred_path_parts[0].split(':')
        if len(cred_part) > 2:
            raise Exception('Unrecognized URL format before \'@\'.  ' +
                            'Too many \':\' symbols.')
        user = cred_part[0]
        if len(cred_part) == 2:
            password = cred_part[1]
    path_parts = cred_path_parts[-1].split(':')
    if len(path_parts) > 2:
        raise Exception('Unrecognized URL format after \'@\'.  ' +
                        'Too many \':\' symbols.')
    dirpath = path_parts[-1]
    if len(path_parts) == 2:
        server = path_parts[0]
        user_server = '%s@%s' % (user, server)
        if password is None:
            password = getpass.getpass('Password (%s): ' % user_server)
        if user_server not in _srvr_pwds:
            _srvr_pwds[user_server] = password
        this_open = partial(ssh_open, srvr=server,
                            usr=user, pwd=_srvr_pwds[user_server])
        fpath = dirpath
    with this_open(fpath) as fin:
        yield fin
