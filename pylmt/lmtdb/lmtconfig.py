#!/usr/bin/env python
# lmtconfig.py <opts>
#*****************************************************************************\
#*  $Id: Makefile,v 1.9 2013/04/22 auselton Exp $
#*****************************************************************************
#*  Copyright (C) 2013 The Regents of the University of California.
#*  Produced at Lawrence Berkeley National Laboratory (cf, DISCLAIMER).
#*  Written by Andrew Uselton <acuselton@lbl.gov> as part of LMT:
#*  Copyright (C) 2007-2010 Lawrence Livermore National Security, LLC.
#*  This module (re)written by Jim Garlick <garlick@llnl.gov>.
#*  UCRL-CODE-232438
#*
#*  This file is part of Lustre Monitoring Tool, version 2.
#*  Authors: H. Wartens, P. Spencer, N. O'Neill, J. Long, J. Garlick
#*  For details, see http://github.com/chaos/lmt.
#*
#*  LMT is free software; you can redistribute it and/or modify it under
#*  the terms of the GNU General Public License (as published by the Free
#*  Software Foundation) version 2, dated June 1991.
#*
#*  LMT is distributed in the hope that it will be useful, but WITHOUT
#*  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#*  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
#*  for more details.
#*
#*  You should have received a copy of the GNU General Public License along
#*  with LMT; if not, write to the Free Software Foundation, Inc.,
#*  59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.
#*****************************************************************************/
#
# Options include:
# -c <conf>  A file with configuration details for DB access and laready recognized names
# -f Kfs>    The (db name of a) filesystem to examine
# -h         A help message
# -v         Print verbose debug messages
# -V         Print the version and exit
#
# - version 0.1
#
# Todo:
# -

import sys
import os
import re
import time
import string
import argparse
import datetime
import MySQLdb

from pyLMT import LMTConfig

#*******************************************************************************

if __name__ == "__main__":
    """
lmtconfig.py <opts>
Options include:
-c <conf>  A file with configuration details for DB access and laready recognized names
-f Kfs>    The (db name of a) filesystem to examine
-h         A help message
-v         Print verbose debug messages
-V         Print the version and exit

2011-08-05
 - version 0.1

Todo:
 -
    """
    DEFAULT_CONFIG=os.path.expanduser('~/.lmtrc')
    if not os.access(DEFAULT_CONFIG, os.R_OK):
        DEFAULT_CONFIG='/project/projectdirs/pma/lmt/etc/lmtrc'
        if not os.access(DEFAULT_CONFIG, os.R_OK):
            print 'No default config file %s' % DEFAULT_CONFIG
            sys.exit(1)
    args = None
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-c', '--config', default=os.path.expanduser(DEFAULT_CONFIG), type=file, help='The configuration file to use for DB access')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) filesystem to examine')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
    args = parser.parse_args()
    config = LMTConfig.LMTConfig(args.config)
    fs = None
    if args.fs != None:
        try:
            fs = config.filesystem(name=args.fs)
        except:
            config.show()
    else:
        try:
            fs = config.filesystem(index=args.index)
        except:
            config.show()
    if fs != None:
        config.show_fs(fs)


