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
matplotlibdir='/var/tmp/matplotlib'
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
import numpy as np
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import h5py

#from pyLMT import LMTConfig, Bulk, MDS, Graph, Timestamp
from pyLMT import Graph

import bottle
# ... build or import your bottle application here ...

#*******************************************************************************
def process_args(host, filesys, begin, end, data):
    """
    see validate_args() below.
    host is most likely 'hopper' at this time
    filesys is going to be 'scratch' or 'scratch2'
    begin and end are ten digit seconds in epoch values
    data is either 'bulk' or 'metadata'
    """
    parser = argparse.ArgumentParser(description='Access an h5lmt file')
    args = parser.parse_args()
    args.host = host
    args.filesys = filesys
    args.begin = begin
    args.end = end
    args.data = data
    args.index = 0
    # Oops. You need to figure out which daily snapshot to draw from.
    lmtDataDir = '/project/projectdirs/pma/www/daily'
    # and we'll assume it doen't straddle midnight for now
    dateStr = time.strftime("%Y-%m-%d", time.localtime(int(args.begin)))
    args.file = "%s/%s/%s_%s.h5lmt" % (lmtDataDir, dateStr, args.host, args.filesys)
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
    if args.begin >= args.end:
        return(None)
    if not args.data in ['bulk','metadata']:
        return(None)
    #args.config = open('/project/projectdirs/pma/lmt/etc/lmtrc')
    args.verbose = False
    return(args)

#*******************************************************************************

def find_sie(sie, dataSet):
    first = 0
    last = len(dataSet) - 1
    while first < last:
        mid = int((first+last)/2)
        if sie == dataSet[mid]:
            return(mid)
        if first == last -1:
            if ((dataSet[first] <= sie) and
                (dataSet[last] > sie)):
                return(first)
            if sie == dataSet[last]:
                return(last)
            print "brw_stats_model_h5lmt.find_sie(): Binary seach for %d failed at (%d, %d). Are there outof order timestamp entries?" % (sie, first, last)
            return(None)
        if sie < dataSet[mid]:
            last = mid
        else:
            first = mid

#*******************************************************************************

def show_bulk(args, fsFile):
    """
    This is taken almost verbatim from $LMT/pylmt/ratefromh5lmt.py:doAction()
    with the small difference that args.begin and args. end are alread seconds
    in epoch, and we want to return the image rather than show it.
    """
    b_sie = int(args.begin)
    e_sie = int(args.end)
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    # we set the data director based on b_sie, so it should be in bounds.
    # we can sanity check it if needed, but that will take a new error mode.
    if e_sie > fsStepsDataSet[-1]:
        e_sie = fsStepsDataSet[-1]
    b_index = find_sie(b_sie, fsStepsDataSet)
    e_index = find_sie(e_sie, fsStepsDataSet)
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    read = np.zeros(e_index - b_index + 1)
    ostReadGroup = fsFile['OSTReadGroup']
    ostBulkReadDataSet = ostReadGroup['OSTBulkReadDataSet']
    ost_index = 0
    for ost_name in ostBulkReadDataSet.attrs['OSTNames']:
        #print "OST %s" % ost_name
        read += ostBulkReadDataSet[ost_index,b_index:e_index+1]
        #print "read:", ostBulkReadDataSet[ost_index,b_index:e_index+1]
        ost_index += 1
    read /= (1024*1024)
    write = np.zeros(e_index - b_index + 1)
    ostWriteGroup = fsFile['OSTWriteGroup']
    ostBulkWriteDataSet = ostWriteGroup['OSTBulkWriteDataSet']
    ost_index = 0
    for ost_name in ostBulkWriteDataSet.attrs['OSTNames']:
        #print "OST %s" % ost_name
        write += ostBulkWriteDataSet[ost_index,b_index:e_index+1]
        #print "write: ", ostBulkWriteDataSet[ost_index,b_index:e_index+1]
        ost_index += 1
    write /= (1024*1024)
    cpu = np.zeros(e_index - b_index + 1)
    ossCPUGroup = fsFile['OSSCPUGroup']
    ossCPUDataSet = ossCPUGroup['OSSCPUDataSet']
    oss_index = 0
    for oss_name in ossCPUDataSet.attrs['OSSNames']:
        cpu += ossCPUDataSet[oss_index,b_index:e_index+1]
        oss_index += 1
    cpu /= oss_index
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, fsStepsDataSet[b_index:e_index+1], read, 'r', label='read', Ave=True)
    Graph.timeSeries(ax, fsStepsDataSet[b_index:e_index+1], write, 'b', label='write', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    (handles, labels) = Graph.percent(ax, fsStepsDataSet[b_index:e_index+1], cpu, color='k', label='% CPU', Ave=True)
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s aggregate I/O" % (fsStepsDataSet.attrs['day'],
                                       fsStepsDataSet.attrs['fs']))
    ax.set_ybound(lower = 0, upper = 50000)
    plt.legend(handles, labels)
    page = StringIO.StringIO()
    plt.savefig(page, format='png')
    page.seek(0)
    plt.cla()
    return(page)

#*******************************************************************************

def show_metadata(args, fsFile):
    """
    """
    b_sie = int(args.begin)
    e_sie = int(args.end)
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    # we set the data director based on b_sie, so it should be in bounds.
    # we can sanity check it if needed, but that will take a new error mode.
    if e_sie > fsStepsDataSet[-1]:
        e_sie = fsStepsDataSet[-1]
    b_index = find_sie(b_sie, fsStepsDataSet)
    e_index = find_sie(e_sie, fsStepsDataSet)
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    mds = np.zeros(e_index - b_index + 1)
    mdsOpsGroup = fsFile['MDSOpsGroup']
    mdsOpsDataSet = mdsOpsGroup['MDSOpsDataSet']
    op_index = 0
    for op_name in mdsOpsDataSet.attrs['OpNames']:
        mds += mdsOpsDataSet[op_index,b_index:e_index+1]
        op_index += 1
    mdsCPUGroup = fsFile['MDSCPUGroup']
    mdsCPUDataSet = mdsCPUGroup['MDSCPUDataSet']
    cpu = mdsCPUDataSet[b_index:e_index+1]
    np.set_printoptions(threshold='nan')
    #print "cpu: ", cpu
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, fsStepsDataSet[b_index:e_index+1], mds, 'g', label='metadata', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$ops/sec$')
    (handles, labels) = Graph.percent(ax, fsStepsDataSet[b_index:e_index+1], cpu, color='k', label='% CPU', Ave=True)
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.title("%s %s Metadata Operations" % (fsStepsDataSet.attrs['day'],
                                             fsStepsDataSet.attrs['fs']))
    ax.set_ybound(lower = 0, upper = 120000)
    if (not handles is None) and (not labels is None):
        plt.legend(handles, labels)
    page = StringIO.StringIO()
    plt.savefig(page, format='png')
    page.seek(0)
    plt.cla()
    return(page)

#*******************************************************************************

# This is just a quick test that the bottle ap is doing anything at all
@bottle.route('/')
def hello_pylmt():
    return(bottle.template('welcome'))

@bottle.route('/<host:re:hopper|grace>/')
@bottle.route('/<host:re:hopper|grace>')
def host(host):
    end = int(time.time())
    begin = end - 600
    return(host_interval(host, begin, end))

@bottle.route('/<host:re:hopper|grace>/<filesys:re:scratch2?>/')
@bottle.route('/<host:re:hopper|grace>/<filesys:re:scratch2?>')
def host_filesys(host, filesys):
    return(bottle.template('host_filesys', host=host, filesys=filesys))

@bottle.route('/<host:re:hopper|grace>/<filesys:re:scratch2?>/<data:re:bulk|metadata>/')
@bottle.route('/<host:re:hopper|grace>/<filesys:re:scratch2?>/<data:re:bulk|metadata>')
def host_filesys_data(host, filesys, data):
    end = int(time.time())
    begin = end - 600
    return(bottle.template('host_filesys_data',
                           host=host,
                           filesys=filesys,
                           begin=begin,
                           end=end,
                           data=data))

# Serve up four graphs for the given system over the given interval
# The begin and end parameters are 10 digit seconds in epoch values
# The template embeds four calls back to this application, one for
# each graph. See the next route.
@bottle.route('/<host:re:hopper|grace>/<begin:re:\d{10}>/<end:re:\d{10}>/')
@bottle.route('/<host:re:hopper|grace>/<begin:re:\d{10}>/<end:re:\d{10}>')
def host_interval(host, begin, end):
    begin_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(begin)))
    if begin_str is None:
        return(bottle.template('bad_value', host=host, filesys='n/a', begin=begin, end=end, data='n/a', value='begin'))
    end_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(end)))
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
@bottle.route('/<host:re:franklin|hopper>/<filesys:re:scratch2?>/<begin:re:\d{10}>/<end:re:\d{10}>/<data:re:bulk|metadata>/')
@bottle.route('/<host:re:franklin|hopper>/<filesys:re:scratch2?>/<begin:re:\d{10}>/<end:re:\d{10}>/<data:re:bulk|metadata>')
def one_graph(host, filesys, begin, end, data):
    args = process_args(host, filesys, begin, end, data)
    if args is None:
        return(bottle.template('pylmt', host=host, filesys=filesys, begin=begin, end=end, data=data))
    #output = 'It is not bulk'
    try:
        fsFile = h5py.File(args.file, 'r')
    except:
        return(bottle.template('bad_value', host=host, filesys=filesys, begin=begin, end=end, data=data, value='bulk'))
    if data == 'bulk':
        try:
            image = show_bulk(args, fsFile)
        except:
            return('Bulk: Image construction failed')
    elif data == 'metadata':
        try:
            image = show_metadata(args, fsFile)
        except:
            return('Metadata: Image construction failed')
    else:
        return(bottle.template('bad_value', host=host, filesys=filesys, begin=begin, end=end, data=data, value='data'))
    fsFile.close()
    if type(image) == str:
        return(image)
    if image is None:
        return(bottle.template('bad_image', host=host, filesys=filesys, begin=begin, end=end, data=data))
    bottle.response.content_type = 'image/png'
    return(image.read())

bottle.debug(True)
#bottle.run(host='localhost', port=18880, reloader=True)
# Do NOT use bottle.run() with mod_wsgi
application = bottle.default_app()
