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
# Put in /etc/httpd/conf.d/wsgi.conf as:
# WSGIScriptAlias /pylmt /project/projectdirs/pma/PYLMT/web/pylmt.wsgi
#
# Code borrowed from newt.wsgi. Manipulate sys.path to make
# PYLMT virtual environment take precedence
import os
import site
import sys

rootpath=os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
webpath=rootpath+"/web"

# Remember original sys.path.
prev_sys_path = list(sys.path)

# Add each new site-packages directory.
site.addsitedir(rootpath+'/lib/python2.6/site-packages')

# Reorder sys.path so new directories at the front.
new_sys_path = []
for item in list(sys.path):
    if item not in prev_sys_path:
        new_sys_path.append(item)
        sys.path.remove(item)
sys.path[:0] = new_sys_path


for i in [rootpath, webpath]:
    sys.path.append(i)

# matplotlib will try to write to this directory and will
# fail to import if it can't
matplotlibdir='/tmp'
if not os.access(matplotlibdir, os.W_OK):
    print "Not allowed to write to %s" % matplotlibdir
    sys.exit()
os.environ['MPLCONFIGDIR'] = matplotlibdir

# http://bottlepy.org/docs/dev/tutorial.html#deployment
# Change working directory so relative paths (and template lookup) work again
os.chdir(webpath)

import re
import time
import StringIO
import argparse
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt

from pyLMT import LMTConfig, Bulk, MDS, Graph, Timestamp

import bottle
# ... build or import your bottle application here ...

#*******************************************************************************
def process_args(host, filesys, begin, end, data):
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    args = parser.parse_args()
    args.host = host
    args.filesys = filesys
    args.begin = begin
    args.end = end
    args.data = data
    args.index = 0
    return(validate_args(args))

#*******************************************************************************
def validate_args(args):
    if not args.host in ['franklin', 'hopper']:
        return(None)
    if not args.filesys in ['scratch','scratch2']:
        return(None)
    if re.match('^\d{10}$', args.begin) == None:
        return(None)
    if re.match('^\d{10}$', args.end) == None:
        return(None)
    if not args.data in ['bulk','metadata']:
        return(None)
    args.config = open('/home/uselton/.lmtrc.remote')
    args.verbose = False
    return(args)

#*******************************************************************************
def get_fsrc(args):
    fsrc = None
    if args.host == 'franklin':
        if args.filesys == 'scratch':
            args.fs = 'filesystem_franklin_scratch_20090420'
            fsrc = LMTConfig.process_configuration(args)
        elif args.filesys == 'scratch2':
            args.fs = 'filesystem_franklin_scratch2_20090420'
            fsrc = LMTConfig.process_configuration(args)
        ymax = 18000
    elif args.host == 'hopper':
        if args.filesys == 'scratch':
            args.fs = 'filesystem_scratch'
            fsrc = LMTConfig.process_configuration(args)
            if fsrc is None:
                return('fsrc is None')
            else:
                return(fsrc)
        elif args.filesys == 'scratch2':
            args.fs = 'filesystem_scratch2'
            fsrc = LMTConfig.process_configuration(args)
        ymax = 50000
    return(fsrc)

#*******************************************************************************
def get_bulk(args):
    fsrc = get_fsrc(args)
    if type(fsrc) is str:
        return(fsrc)
    bulk = Bulk.Bulk(fsrc['name'])
    bulk.getOSSs(fsrc['conn'])
    (begin_ts, end_ts) = Timestamp.process_timestamps(args, fsrc)
    bulk.getQuickData(begin_ts,
                         end_ts,
                         conn=fsrc['conn'])
    return(bulk)

#*******************************************************************************

def show_bulk(args, bulk):
    """
    It would be nice if I could unify this with Graph.bulk_plot().That
    would entail using the StringIO function there and handling the
    save/show option in the caller.
    """
    scale = 1024.0*1024.0
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = bulk.Steps.Steps
    ybound = 50000.0
    values = bulk.Read.Values/scale
    Graph.timeSeries(ax, steps, values, 'r', label='read', Ave=True)
    values = bulk.Write.Values/scale
    Graph.timeSeries(ax, steps, values, 'b', label='write', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    if not bulk.CPU is None:
        values = bulk.CPU.Values
        (handles, labels) = Graph.percent(ax, steps, values, 'k', label='% CPU', Ave=True)
        plt.legend(handles, labels)
    else:
        plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s %s aggregate I/O" % (dayStr, bulk.name))
    ax.set_ybound(lower = 0, upper = ybound)
    page = StringIO.StringIO()
    plt.savefig(page, format='png')
    page.seek(0)
    plt.cla()
    return(page)

#*******************************************************************************
def get_metadata(args):
    fsrc = get_fsrc(args)
    if fsrc is None:
        return(None)
    metadata = MDS.MDS(host=fsrc['host'], fs=fsrc['name'])
    metadata.opsFromDB(fsrc['conn'])
    (begin_ts, end_ts) = Timestamp.process_timestamps(args, fsrc)
    metadata.getQuickData(begin_ts,
                         end_ts,
                         conn=fsrc['conn'])
    metadata.getCPU()
    return(metadata)

#*******************************************************************************

def show_metadata(args, metadata):
    """
    It would be nice if I could unify this with Graph.mds_plot().
    """
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = metadata.Steps.Steps
    values = metadata.MDS.Values
    Graph.timeSeries(ax, steps, values, 'b', label='ops', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$ops/sec$')
    if not metadata.CPU is None:
        values = metadata.CPU.Values
        (handles, labels) = Graph.percent(ax, steps, values, 'k', label='% CPU', Ave=True)
        plt.legend(handles, labels)
    else:
        plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(metadata.begin.sie))
    plt.title("%s %s metadata" % (dayStr, metadata.name))
    page = StringIO.StringIO()
    plt.savefig(page, format='png')
    page.seek(0)
    plt.cla()
    return(page)

#*******************************************************************************

# This is just a quick test that the bottle ap is doing anything at all
@bottle.route('/pylmt')
def hello_pylmt():
    return("Hello pylmt!")

# Serve up four graphs for the given system over the given interval
# The begin and end parameters are 10 digit seconds in epoch values
# The template embeds four calls back to this application, one for
# each graph. See the next route.
@bottle.route('/pylmt/<host:re:franklin|hopper>/<begin:re:\d{10}>/<end:re:\d{10}>')
def host_interval(host, begin, end):
    begin_str = Timestamp.format_timestamp(int(begin))
    if begin_str is None:
        return(bottle.template('bad_value', host=host, filesys='n/a', begin=begin, end=end, data='n/a', value='begin'))
    end_str = Timestamp.format_timestamp(int(end))
    if end_str is None:
        return(bottle.template('bad_value', host=host, filesys='n/a', begin=begin, end=end, data='n/a', value='end'))
    bulk_scratch = '/pylmt/%s/scratch/%s/%s/bulk' % (host, begin, end)
    bulk_scratch2 = '/pylmt/%s/scratch2/%s/%s/bulk' % (host, begin, end)
    metadata_scratch = '/pylmt/%s/scratch/%s/%s/metadata' % (host, begin, end)
    metadata_scratch2 = '/pylmt/%s/scratch2/%s/%s/metadata' % (host, begin, end)
    return(bottle.template('host_interval',
                    host=host,
                    begin=begin_str,
                    end=end_str,
                    bulk_scratch=bulk_scratch,
                    bulk_scratch2=bulk_scratch2,
                    metadata_scratch=metadata_scratch,
                    metadata_scratch2=metadata_scratch2))

# Serve up one of the requested graphs
@bottle.route('/pylmt/<host:re:franklin|hopper>/<filesys:re:scratch2?>/<begin:re:\d{10}>/<end:re:\d{10}>/<data:re:bulk|metadata>')
def one_graph(host, filesys, begin, end, data):
    args = process_args(host, filesys, begin, end, data)
    if args is None:
        return(bottle.template('pylmt', host=host, filesys=filesys, begin=begin, end=end, data=data))
    output = 'It is not bulk'
    if data == 'bulk':
        B = get_bulk(args)
        if B is None:
            return(bottle.template('bad_value', host=host, filesys=filesys, begin=begin, end=end, data=data, value='bulk'))
        if type(B) is str:
            return[B]
        try:
            image = show_bulk(args, B)
        except:
            return('Bulk: Image construction failed')
    elif data == 'metadata':
        M = get_metadata(args)
        if M is None:
            return(bottle.template('bad_value', host=host, filesys=filesys, begin=begin, end=end, data=data, value='metadata'))
        if type(M) is str:
            return[M]
        try:
            image = show_metadata(args, M)
        except:
            return('Metadata: Image construction failed')
    else:
        return(bottle.template('bad_value', host=host, filesys=filesys, begin=begin, end=end, data=data, value='data'))
    if image is None:
        return(bottle.template('bad_image', host=host, filesys=filesys, begin=begin, end=end, data=data))
    bottle.response.content_type = 'image/png'
    return(image.read())

bottle.debug(True)
bottle.run(host='localhost', port=18880, reloader=True)
# Do NOT use bottle.run() with mod_wsgi
#application = bottle.default_app()
