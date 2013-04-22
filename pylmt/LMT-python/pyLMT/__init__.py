"""
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
"""

__revision__ = """$Revision: 001 $"""[11:-2]
from release import __version__, version_info, __author__

import os
from _pylmt_exceptions import DefaultConfigError

from _defaultrc import DEFAULT_LMTRC
# The the system-wide DEFAULT_CONFIG is used if the .lmtrc file is
# not in your home directory (or not readable). The system-wide
# value is set in site.cfg prior to running setup.py.
DEFAULT_CONFIG=os.path.expanduser('~/.lmtrc')
if not os.access(DEFAULT_CONFIG, os.R_OK):
    DEFAULT_CONFIG=DEFAULT_LMTRC
    if not os.access(DEFAULT_CONFIG, os.R_OK):
        raise DefaultConfigError("Can't access default configuration file at %s" % DEFAULT_CONFIG)

def tok(words=None):
    """
    This is just a silly litte function to have something
    to test as I build out the package.
    """
    if words is None:
        print "Nothing to say"
        return
    if type(words) is str:
        print words
        return
    if type(words) is list:
        for word in words:
            print word
        return
    print "Don't know how to say %s" % str(words)
    return

def defaultErrorHandler(obj, error, message):
    """
    The check about if the obj is actually an object might be better
    as an assert. Similar issue with whether it actually has an
    ErrorMessages attribut.
    And we can assert that error is in fact an Error object.
    """
    if isinstance(obj, object) and hasattr(obj, 'ErrorMessages'):
        if (obj.ErrorMessages is None):
            obj.ErrorMessages = ""
        obj.ErrorMessages += message
    raise error(message)

__all__=['Bulk', 'Counter', 'CPU', 'FS', 'Graph', 'LMTConfig', 'MDS',
         'Operation', 'OSS', 'OST', 'Series', 'Statistics', 'tests',
         'TimeSeries', 'Timestamp', 'TimeSteps']

