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
import os
import time
import datetime
import numpy as np
import matplotlib as mpl
# If this is run from a cron job it is not a login process and
# has no X $DISPLAY in the environment. You can prevent pyplot
# from getting confused by telling matplotlib to use the 'Agg'
# backend. On the other hand, if you've already loaded pyplot
# or pylab then it's too late to use the mpl.use('Agg') and would
# generate an obnoxious warning if you try. N.B. The backend
# property is not case sensitive and get_backend() actually
# returns a lower case string.
backend = mpl.get_backend()
if (not 'DISPLAY' in os.environ) or (os.environ['DISPLAY'] == None):
#    print "Bulk: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt

from pyLMT import Timestamp, defaultErrorHandler

from _pylmt_exceptions import Error

handleError = defaultErrorHandler

class GraphError(Error):
    """
    Generic Error for problems with Graphs.
    """
class GraphArrayMismatchError(Error):
    """
    Arrays for the x and y values are passed in separately, but mut
    be the same length.
    """

#*******************************************************************************
def timeSeries(ax, times, values, color='k', label=None, Ave=False, format=None):
    """
    Provide and axes object and times and values as from a TimeSeries object.
    Plot the time series values on the axes with suitably organized date values.
    This relies on the Timestamp module. Use the indicated color. Include the
    label if any. Also plot the average as a black dotted line if requested. Note
    that the values should already be scaled when delivered.
    """
    ltimes = len(times)
    lvalues = len(values)
    if (ltimes <= 1) or (ltimes != lvalues):
        raise GraphArrayMismatchError("Graph.timeSeries(): Warning - %d times and %d values." % (ltimes, lvalues))
        # not reached
    # plot date takes a float array of days since epoc
    dates = np.array(times, dtype=np.float64)
    if format is None:
        if ltimes <= 500:
            format = color+'-'
        else:
            # the ',' make the point one pixel
            # I found that the ',' makes them too small and faint. Without mec=color
            # (marker edge color) the balck edge dominates.
            format = color+','
    if time.localtime(times[0]).tm_isdst == 1:
        tzAdjust = 7.0
    else:
        tzAdjust = 8.0
    dates = (dates - tzAdjust*60.0*60.0)/(24.0*60.0*60.0) + mpl.dates.date2num(datetime.date(1970,1,1))
    if Ave == True:
        ave = np.ones_like(dates)*np.average(values)
        ax.plot_date(dates, ave, fmt=color+'--', xdate=True, ydate=False, label='ave', zorder=1)
    ax.plot_date(dates, values, fmt=format, mec=color, xdate=True, ydate=False, label=label, zorder=0)
    return

#*******************************************************************************
def percent(ax, times, values, color='k', label="pct CPU", Ave=False, format=None, ybound=None):
    """
    Provide and axes object and times and values as from a CPU object.
    Create secondary axes with separately scaled y-axis. Plot the time series
    values on the new axes.
    This relies on the Timestamp module. Use the indicated color. Include the
    label if any. Also plot the average as a black dotted line if requested. Note
    that the values should already be scaled when delivered.
    """
    ltimes = len(times)
    lvalues = len(values)
    if (ltimes <= 1) or (ltimes != lvalues):
        raise GraphArrayMismatchError("Graph.percent(): Warning - %d times and %d values." % (ltimes, lvalues))
        # not reached
    if ybound is None:
        ymax = 100
        if np.max(values) > ymax:
            ymax = 100*(int(np.max(values)/100.0) + 1)
    else:
        ymax = ybound
    handles1,labels1 = ax.get_legend_handles_labels()
    # plot date takes a float array of days since epoc
    dates = np.array(times, dtype=np.float64)
    if format is None:
        if ltimes <= 500:
            format = '-'
        else:
            # the ',' make the point one pixel
            format = ','
    if time.localtime(times[0]).tm_isdst == 1:
        tzAdjust = 7.0
    else:
        tzAdjust = 8.0
    dates = (dates - tzAdjust*60.0*60.0)/(24.0*60.0*60.0) + mpl.dates.date2num(datetime.date(1970,1,1))
    ax2 = ax.twinx()
    if Ave == True:
        ave = np.ones_like(dates)*np.average(values)
        ax2.plot_date(dates, ave, fmt='--', xdate=True, ydate=False, color=color, mec=color, label='ave', zorder=1)
    ax2.plot_date(dates, values, fmt=format, xdate=True, ydate=False, color=color, label=label, zorder=0)
    ax2.set_ybound(lower = 0, upper = ymax)
    ax2.set_ylabel(label)
    handles2,labels2 = ax2.get_legend_handles_labels()
    handles1 += handles2
    labels1 += labels2
    return(handles1, labels1)

#*******************************************************************************

def scatter(ax, xvals, yvals, color, label=None):
    """
    Produce a scatter plot of the value pairs.
    """
    xlen = len(xvals)
    ylen = len(yvals)
    if (xlen <= 1) or (xlen != ylen):
        raise GraphArrayMismatchError("Graph.scatter(): Warning - %d x values and %d y values." % (xlen, ylen))
        # not reached
    ax.plot(xvals, yvals, color+',', label=label)
    return

#*******************************************************************************

def bar(ax, xvals, yvals, width=0.35, offset=0.0, color='r', label=None):
    """
    Produce a scatter plot of the value pairs.
    """
    xlen = len(xvals)
    ylen = len(yvals)
    if (xlen <= 1) or (xlen != ylen):
        raise GraphArrayMismatchError("Graph.bar(): Warning - %d bins and %d values." % (xlen, ylen))
        # not reached
    ax.bar(xvals+offset, yvals, width=width, color=color, label=label)
    return

#*******************************************************************************
# Bulk plots
#*******************************************************************************
def bulk_plot(bulk, mode=None, plot=None, ybound=None, scale=1024.0*1024.0, withCPU=True):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = bulk.Steps.Steps
    ymax = 0
    if mode == 'Both':
        values = bulk.Bulk.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        timeSeries(ax, steps, values, 'b', label='read and write', Ave=True)
    elif mode is None:
        values = bulk.Read.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        timeSeries(ax, steps, values, 'r', label='read', Ave=True)
        values = bulk.Write.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        timeSeries(ax, steps, values, 'b', label='write', Ave=True)
    elif mode == 'Read':
        values = bulk.Read.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        timeSeries(ax, steps, values, 'r', label='read', Ave=True)
    else:
        values = bulk.Write.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        timeSeries(ax, steps, values, 'b', label='write', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    if (withCPU == True) and (not bulk.CPU is None):
        values = bulk.CPU.Values
        (handles, labels) = percent(ax, steps, values, 'k', label='% CPU', Ave=True)
        if (not handles is None) and (not labels is None):
            plt.legend(handles, labels)
    else:
        plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s %s aggregate I/O" % (dayStr, bulk.name))
    if ybound is None:
        ybound = ymax
    ax.set_ybound(lower = 0, upper = ybound)
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************

def bulk_xcorr(bulk, mode=None, plot=None, ybound=None, scale=1024*1024):
    """
    The scatter plot of the aggregate I/O rates versus average CPU utilization
    is less illumination than the composite of all the individual OSS scatter
    plots. I don't call on this one, but it is still here and available.
    """
    if not ((mode == 'Read') or (mode == 'Write') or (mode == 'Both') or (mode is None)):
        return
    if bulk.CPU is None:
        return
    if ((bulk.Read is None) or (bulk.Write is None) or (bulk.Bulk is None)):
        return(None)
    if ybound is None:
        if mode == 'Read':
            ymax = bulk.Read.getMax()/scale
        elif mode == 'Write':
            ymax = bulk.Write.getMax()/scale
        elif mode == 'Both':
            ymax = bulk.Bulk.getMax()/scale
        else:
            readMax = bulk.Read.getMax()/scale
            writeMax = bulk.Write.getMax()/scale
            if readMax > writeMax:
                ymax = readMax
            else:
                ymax = writeMax
        ybound = ymax
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if (mode is None) or (mode == 'Read'):
        scatter(ax, bulk.CPU.Values, bulk.Read.Values/scale, 'r', label="read")
    if (mode is None) or (mode == 'Write'):
        scatter(ax, bulk.CPU.Values, bulk.Write.Values/scale, 'b', label="write")
    if mode == 'Both':
        scatter(ax, bulk.CPU.Values, bulk.Bulk.Values/scale, 'b', label="read+write")
    ax.set_xbound(lower = 0, upper = 100)
    ax.set_ybound(lower = 0, upper = ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s Bulk %s activity vs %%CPU" % (dayStr, bulk.name))
    ax.set_xlabel(r"$\%$ CPU")
    ax.set_ylabel(r"$MB/sec$")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************

def bulk_composite_xcorr(bulk, mode=None, plot=None, ybound=None, scale=1024.0*1024.0):
    if ybound is None:
        ymax = 0.0
        for oss in bulk.OSSs:
            if mode == 'Read':
                ossMax = oss.Read.getMax()/scale
            elif mode == 'Write':
                ossMax = oss.Write.getMax()/scale
            elif mode == 'Both':
                ossMax = oss.OSS.getMax()/scale
            else:
                readMax = oss.Read.getMax()/scale
                writeMax = oss.Write.getMax()/scale
                if readMax > writeMax:
                    ossMax = readMax
                else:
                    ossMax = writeMax
            if ossMax > ymax:
                ymax = ossMax
        ybound = ymax
    if ybound <= 0.0:
        return
    fig = plt.figure()
    ax = fig.add_subplot(111)
    handles = None
    labels = None
    for oss in bulk.OSSs:
        if (mode is None) or (mode == 'Read'):
            scatter(ax, oss.CPU.Values, oss.Read.Values/scale, 'r', label='read')
        if (mode is None) or (mode == 'Write'):
            scatter(ax, oss.CPU.Values, oss.Write.Values/scale, 'b', label='write')
        if mode == 'Both':
            scatter(ax, oss.CPU.Values, oss.OSS.Values/scale, 'b', label='read+write')
        if (handles is None) and (labels is None):
            (handles, labels) = ax.get_legend_handles_labels()
    plt.legend(handles, labels)
    ax.set_xbound(lower = 0, upper = 100)
    ax.set_ybound(lower = 0, upper = ybound)
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s Composite %s OSS activity vs %%CPU" % (dayStr, bulk.name))
    ax.set_xlabel(r"$\%$ CPU")
    ax.set_ylabel(r"$MB/sec$")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************

def bulk_sdevs(bulk, mode=None, plot=None, ybound=None, scale=1024.0*1024.0):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    for oss in bulk.OSSs:
        if (mode is None) or (mode == 'Read'):
            (ave, sdev) = oss.CalcSdev('Read', scale=scale)
            if (not ave is None) and (not sdev is None):
                coefOfVar = np.zeros_like(sdev)
                coefOfVar[ave != 0.0] = sdev[ave != 0.0]/ave[ave != 0.0]
                ax.plot(coefOfVar, ave, ',r', label='read', zorder=0)
        if (mode is None) or (mode == 'Write'):
            (ave, sdev) = oss.CalcSdev('Write', scale=scale)
            if (not ave is None) and (not sdev is None):
                coefOfVar = np.zeros_like(sdev)
                coefOfVar[ave != 0.0] = sdev[ave != 0.0]/ave[ave != 0.0]
                ax.plot(coefOfVar, ave, ',b', label='write', zorder=0)
        if mode == 'Both':
            (ave, sdev) = oss.CalcSdev('Both', scale=scale)
            if (not ave is None) and (not sdev is None):
                coefOfVar = np.zeros_like(sdev)
                coefOfVar[ave != 0.0] = sdev[ave != 0.0]/ave[ave != 0.0]
                ax.plot(coefOfVar, ave, ',b', label='read + write', zorder=0)
    if not ybound is None:
        ax.set_ybound(lower = 0, upper = ybound)
    l = plt.axvline(x=np.sqrt(5.0), color='k', linestyle='--')
    ax.annotate(r'$\sqrt{5}$', xy=(np.sqrt(5.0), 1),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(2.0), color='k', linestyle='--')
    (bottom, top) = ax.get_ybound()
    ax.annotate(r'$\sqrt{2}$', xy=(np.sqrt(2.0), 0.99*top),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(1.0), color='k', linestyle='--')
    l = plt.axvline(x=np.sqrt(0.5), color='k', linestyle='--')
    ax.annotate(r'$\sqrt{1/2}$', xy=(np.sqrt(0.5), 1),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(0.2), color='k', linestyle='--')
    ax.annotate(r'$\sqrt{1/5}$', xy=(np.sqrt(0.2), 1),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(0.0), color='k', linestyle='--')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s %s coefficinet of variation vs average rate" % (dayStr, bulk.name))
    plt.xlabel('CoV')
    plt.ylabel(r'average $MiB/sec$ across OSTs (per OSS)')
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************

def bulk_composite_spectrum(bulk, mode, plot, ybound):
    maxRate = 2.0*1024*1024*1024
    maxCPU = 100.0
    fig = plt.figure()
    ax = fig.add_subplot(111)
    read = None
    write = None
    both = None
    for oss in bulk.OSSs:
        cpu = oss.CPU.Values/maxCPU
        if (mode == 'Read') or (mode is None):
            rate = oss.Read.Values/maxRate
            ratio = np.zeros_like(rate)
            ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
            spectrum = np.arctan(ratio)
            hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
            if read is None:
                read = hist
            else:
                read += hist
        if (mode == 'Write') or (mode is None):
            rate = oss.Write.Values/maxRate
            ratio = np.zeros_like(rate)
            ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
            spectrum = np.arctan(ratio)
            hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
            if write is None:
                write = hist
            else:
                write += hist
        if mode == 'Both':
            rate = oss.OSS.Values/maxRate
            ratio = np.zeros_like(rate)
            ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
            spectrum = np.arctan(ratio)
            hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
            if both is None:
                both = hist
            else:
                both += hist
    if (mode == 'Read') or (mode is None):
        ax.plot(bins[1:-1], read[1:], 'r-', label='Read')
    if (mode == 'Write') or (mode is None):
        ax.plot(bins[1:-1], write[1:], 'b-', label='Write')
    if mode == 'Both':
        ax.plot(bins[1:-1], both[1:], 'b-', label='Both')
    l = plt.axvline(x=np.pi/2.0, color='k', linestyle='--')
    ax.annotate(r'$\pi/2$', xy=(np.pi/2, 1),  xycoords='data',
                xytext=(-50, 30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    ax.set_xbound(lower = 0, upper = 1.6)
    if not ybound is None:
        ax.set_ybound(lower = 0, upper=ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s %s rate/CPU spectrum" % (dayStr, bulk.name))
    ax.set_xlabel(r"arctan($((MB/s)/2000)/(\%$ CPU))")
    ax.set_ylabel(r"count")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************

def bulk_spectrum(bulk, mode, plot, ybound):
    maxRate = 50.0*1024*1024*1024
    maxCPU = 100.0
    fig = plt.figure()
    ax = fig.add_subplot(111)
    cpu = bulk.CPU.Values/maxCPU
    if (mode == 'Read') or (mode is None):
        rate = bulk.Read.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'r-', label='Read')
    if (mode == 'Write') or (mode is None):
        rate = bulk.Write.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'b-', label='Write')
    if mode == 'Both':
        rate = bulk.Bulk.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'b-', label='Both')
    l = plt.axvline(x=np.pi/2.0, color='k', linestyle='--')
    ax.annotate(r'$\pi/2$', xy=(np.pi/2, 1),  xycoords='data',
                xytext=(-50, 30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    ax.set_xbound(lower = 0, upper = 1.6)
    if not ybound is None:
        ax.set_ybound(lower = 0, upper=ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s %s rate/CPU spectrum" % (dayStr, bulk.name))
    ax.set_xlabel(r"arctan($((MB/s)/2000)/(\%$ CPU))")
    ax.set_ylabel(r"count")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************
#CPU plots
#*******************************************************************************
def CPU_plot(cpu, plot):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = cpu.Steps.Steps
    timeSeries(ax, steps, np.zeros_like(steps), 'b')
    ax.set_ybound(lower=0, upper=100)
    values = cpu.Values
    (handles, labels) = percent(ax, steps, values, 'k', label='% CPU', Ave=True)
    if (handles != None) and (labels != None):
        plt.legend(handles, labels)
    plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.xlabel('time')
    plt.title("Percent CPU utilization")
    if plot == None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************
# MDS plots
#*******************************************************************************
def MDS_plot(mds, plot, ymax):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = mds.Steps.Steps
    values = mds.MDS.Values
    if ymax is None:
        ymax = np.max(values)
    timeSeries(ax, steps, values, 'b', label='MDS', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$ops/sec$')
    if not mds.CPU is None:
        values = mds.CPU.Values
        (handles, labels) = percent(ax, steps, values, 'k', label='% CPU', Ave=True)
        if (not handles is None) and (not labels is None):
            plt.legend(handles, labels)
    else:
        plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    start_time = steps[0]/(24.0*60.0*60.0) + mpl.dates.date2num(datetime.date(1970,1,1))
    plt.title("%s metadata operations for %s" %
              (mds.name,
               mpl.dates.num2date(start_time).strftime("%Y-%m-%d"))
              )
    if ymax is None:
        ymax = ymax
    ax.set_ybound(lower=0, upper=ymax)
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************
def MDS_xcorr(mds, plot=None, ybound=None, hilite="open"):
    if mds.CPU is None:
        return
    if mds.MDS is None:
        return
    if ybound is None:
        ybound = mds.MDS.getMax()
    fig = plt.figure()
    ax = fig.add_subplot(111)
    scatter(ax, mds.CPU.Values, mds.MDS.Values, 'b', label="mds ops")
    if not hilite is None:
        n = 0
        op_index = None
        for op in mds.Ops:
            if ((op.Values is None) or (op.Steps is None) or
                (op.Steps.steps() == 0) or (op.Stats is None)):
                continue
            if op.name == hilite:
                op_index = n
            n += 1
        if n > 0:
            # mds.x is teh result of a generalized linear regression apporioning
            # the fraction of CPU utilization to each operation. x[n] is the
            # final category, of "no operation".
            if mds.x is None:
                mds.attribute()
            if (not op_index is None) and (mds.x[op_index] != 0.0):
                slope = (100 - mds.x[n])/(100*mds.x[op_index])
                model_x = np.array(range(101))
                model_y = mds.x[n]/100.0 + slope*model_x
                ax.plot(model_x, model_y, "r-", label=hilite)
    ax.set_xbound(lower = 0, upper = 100)
    ax.set_ybound(lower = 0, upper = ybound)
    ax.legend()
    plt.title("%s activity vs %%CPU" % mds.name)
    ax.set_xlabel(r"$\%$ CPU")
    ax.set_ylabel(r"$ops/sec$")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************
def MDS_spectrum(mds, plot, ybound=None):
    maxRate = 125000.0
    maxCPU = 100.0
    fig = plt.figure()
    ax = fig.add_subplot(111)
    cpu = mds.CPU.Values/maxCPU
    rate = mds.MDS.Values/maxRate
    ratio = np.zeros_like(rate)
    ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
    spectrum = np.arctan(ratio)
    hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
    ax.plot(bins[1:-1], hist[1:], 'b-', label='MDS ops')
    l = plt.axvline(x=np.pi/2.0, color='k', linestyle='--')
    ax.annotate(r'$\pi/2$', xy=(np.pi/2, 1),  xycoords='data',
                xytext=(-50, 30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    ax.set_xbound(lower = 0, upper = 1.6)
    if not ybound is None:
        ax.set_ybound(lower = 0, upper=ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(mds.begin.sie))
    plt.title("%s %s rate/CPU spectrum" % (dayStr, mds.name))
    ax.set_xlabel(r"arctan($((Ops/s)/125000)/(\%$ CPU))")
    ax.set_ylabel(r"count")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************
# Operation plots
#*******************************************************************************
def Operation_plot(op, plot, ybound):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = op.Steps.Steps
    values = op.Values
    ymax = np.max(values)
    timeSeries(ax, steps, values, 'b', label=op.name, Ave=True)
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.xlabel('time')
    plt.ylabel('%s/sec' % op.name)
    plt.legend()
    plt.title("%s operations" % (op.name))
    if ybound == None:
        ybound = ymax
    ax.set_ybound(lower=0, upper=ybound)
    if plot == None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************
# OSS plots
#*******************************************************************************
def OSS_plot(oss, mode=None, plot=None, ybound=None, scale=1024.0*1024.0):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = oss.Steps.Steps
    ymax = 0
    if mode == 'Both':
        values = oss.OSS.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        timeSeries(ax, steps, values, 'b', label='read and write', Ave=True)
    elif mode is None:
        values = oss.Read.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        timeSeries(ax, steps, values, 'r', label='read', Ave=True)
        values = oss.Write.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        timeSeries(ax, steps, values, 'b', label='write', Ave=True)
    elif mode == 'Read':
        values = oss.Read.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        timeSeries(ax, steps, values, 'r', label='read', Ave=True)
    else:
        values = oss.Write.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        timeSeries(ax, steps, values, 'b', label='write', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    if not oss.CPU is None:
        values = oss.CPU.Values
        (handles, labels) = percent(ax, steps, values, 'k', label='% CPU', Ave=True)
        if (not handles is None) and (not labels is None):
            plt.legend(handles, labels)
    else:
        plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(oss.begin.sie))
    plt.title("%s %s aggregate I/O" % (dayStr, oss.name))
    if ybound is None:
        ybound = ymax
    ax.set_ybound(lower = 0, upper = ybound)
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************

def OSS_xcorr(oss, mode=None, plot=None, ybound=None, scale=1024.0*1024.0):
    if not ((mode == 'Read') or (mode == 'Write') or (mode == 'Both') or (mode is None)):
        return
    if oss.CPU is None:
        return
    if ((oss.Read is None) or (oss.Write is None) or (oss.OSS is None)):
        return(None)
    if ybound is None:
        if mode == 'Read':
            ymax = oss.Read.getMax()/scale
        elif mode == 'Write':
            ymax = oss.Write.getMax()/scale
        elif mode == 'Both':
            ymax = oss.OSS.getMax()/scale
        else:
            readMax = oss.Read.getMax()/scale
            writeMax = oss.Write.getMax()/scale
            if readMax > writeMax:
                ymax = readMax
            else:
                ymax = writeMax
        ybound = ymax
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if (mode is None) or (mode == 'Read'):
        scatter(ax, oss.CPU.Values, oss.Read.Values/scale, 'r', label="read")
    if (mode is None) or (mode == 'Write'):
        scatter(ax, oss.CPU.Values, oss.Write.Values/scale, 'b', label="write")
    if mode == 'Both':
        scatter(ax, oss.CPU.Values, oss.OSS.Values/scale, 'b', label="read+write")
    ax.set_xbound(lower = 0, upper = 100)
    ax.set_ybound(lower = 0, upper = ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(oss.begin.sie))
    plt.title("%s OSS %s activity vs %%CPU" % (dayStr, oss.name))
    ax.set_xlabel(r"$\%$ CPU")
    ax.set_ylabel(r"$MB/sec$")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************
def OSS_sdevs(oss, mode, plot, ymax=None, scale=1024.0*1024.0):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if (mode is None) or (mode == 'Read'):
        (ave, sdev) = oss.CalcSdev('Read', scale=scale)
        if (not ave is None) and (not sdev is None):
            coefOfVar = np.zeros_like(sdev)
            coefOfVar[ave != 0.0] = sdev[ave != 0.0]/ave[ave != 0.0]
            ax.plot(coefOfVar, ave, ',r', label='read', zorder=0)
    if (mode is None) or (mode == 'Write'):
        (ave, sdev) = oss.CalcSdev('Write', scale=scale)
        if (not ave is None) and (not sdev is None):
            coefOfVar = np.zeros_like(sdev)
            coefOfVar[ave != 0.0] = sdev[ave != 0.0]/ave[ave != 0.0]
            ax.plot(coefOfVar, ave, ',b', label='write', zorder=0)
    if mode == 'Both':
        (ave, sdev) = oss.CalcSdev('Both', scale=scale)
        if (not ave is None) and (not sdev is None):
            coefOfVar = np.zeros_like(sdev)
            coefOfVar[ave != 0.0] = sdev[ave != 0.0]/ave[ave != 0.0]
            ax.plot(coefOfVar, ave, ',b', label='read + write', zorder=0)
    if not ymax is None:
        ax.set_ybound(lower = 0, upper = ymax)
    l = plt.axvline(x=np.sqrt(5.0), color='k', linestyle='--')
    ax.annotate(r'$\sqrt{5}$', xy=(np.sqrt(5.0), 1),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(2.0), color='k', linestyle='--')
    (bottom, top) = ax.get_ybound()
    ax.annotate(r'$\sqrt{2}$', xy=(np.sqrt(2.0), 0.99*top),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(1.0), color='k', linestyle='--')
    l = plt.axvline(x=np.sqrt(0.5), color='k', linestyle='--')
    ax.annotate(r'$\sqrt{1/2}$', xy=(np.sqrt(0.5), 1),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(0.2), color='k', linestyle='--')
    ax.annotate(r'$\sqrt{1/5}$', xy=(np.sqrt(0.2), 1),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(0.0), color='k', linestyle='--')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(oss.begin.sie))
    plt.title("%s OSS %s Coefficient of Variation" % (dayStr, oss.name))
    plt.xlabel('CoV')
    plt.ylabel(r'average $MiB/sec$ across OSTs')
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************

def OSS_spectrum(oss, mode, plot, ybound):
    maxRate = 2.0*1024*1024*1024
    maxCPU = 100.0
    fig = plt.figure()
    ax = fig.add_subplot(111)
    cpu = oss.CPU.Values/maxCPU
    if (mode == 'Read') or (mode is None):
        rate = oss.Read.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'r-', label='Read')
    if (mode == 'Write') or (mode is None):
        rate = oss.Write.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'b-', label='Write')
    if mode == 'Both':
        rate = oss.OSS.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'b-', label='Both')
    l = plt.axvline(x=np.pi/2.0, color='k', linestyle='--')
    ax.annotate(r'$\pi/2$', xy=(np.pi/2, 1),  xycoords='data',
                xytext=(-50, 30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    ax.set_xbound(lower = 0, upper = 1.6)
    if not ybound is None:
        ax.set_ybound(lower = 0, upper=ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(oss.begin.sie))
    plt.title("%s %s rate/CPU spectrum" % (dayStr, oss.name))
    ax.set_xlabel(r"arctan($((MB/s)/2000)/(\%$ CPU))")
    ax.set_ylabel(r"count")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************

def OSS_pie(oss, sie, mode, plot):
    step = oss.Steps.getIndex(sie)
    pie = np.zeros(len(oss.OSTs))
    names = []
    i = 0
    for ost in oss.OSTs:
        names.append(ost.name)
        if mode == "Read":
            ts = ost.Read
        elif mode == "Write":
            ts = ost.Write
        else:
            ts = ost.OST
        pie[i] = ts.Values[step]/(1024.0*1024.0)
        i += 1
    fig = plt.figure(figsize=(8,8))
    ax = plt.axes([0.1, 0.1, 0.8, 0.8])
    plt.pie(pie, labels=names)
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

#*******************************************************************************
# OST plots
#*******************************************************************************
def OST_plot(ost, Plots, plot, ybound):
    Steps = ost.Steps.Steps
    fig = plt.figure()
    ax = fig.add_subplot(111)
    for Plot in Plots:
        timeSeries(ax, Steps, Plot['values'], Plot['color'], Plot['label'], Ave=True)
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    plt.legend()
    plt.title("%s I/O" % (ost.name))
    ax.set_ybound(lower=0, upper=ybound)
    if plot == None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    return

def BrwOST_hist(name, bins, units, Plots, plot):
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    width = 0.35
    x = np.arange(len(bins))
    offset=0.0
    for p in Plots:
        bar(ax, x, p['values'], width=width, offset=offset, color=p['color'], label=p['label'])
        offset += width
    ax.set_ylabel('Count')
    ax.set_xlabel(units)
    ax.set_title('%s histogram' % name)
    ax.set_xticks(x+width)
    ax.set_xticklabels( bins, rotation=45, horizontalalignment='right' )
    ax.legend()
    if plot is None:
        plt.show()
    else:
        plt.savefig( name + '_' + plot )
    plt.cla()

def BrwOST_hist_array(name, bins, units, PlotsArray, plot):
    width = 0.35
    x = np.arange(len(bins))
    n = len(PlotsArray)
    fig = plt.figure(figsize=(n*10,10))
    fig.subplots_adjust(bottom=0.2)
    for index, Plots in enumerate(PlotsArray):
        ax = fig.add_subplot(1,n,index)
        offset = 0.0
        for p in Plots:
            bar(ax, x, p['values'], width=width, offset=offset, color=p['color'], label=p['label'])
        offset += width
        ax.set_ylabel('Count')
        ax.set_xlabel(units)
        ax.set_title('%s %s histogram' % (p['ost'], name))
        ax.set_xticks(x+width)
        ax.set_xticklabels( bins, rotation=45, horizontalalignment='right' )
        ax.legend()
    if plot is None:
        plt.show()
    else:
        plt.savefig( name + '_' + plot )
    plt.cla()

def BrwOST_hist_array2(name, bins, units, PlotsArray, plot, yMax=None):
    width = 0.35
    x = np.arange(len(bins))
    nOSSs = len(PlotsArray)
    if nOSSs <= 0:
        #print "Graph:BrwOST_hist_array2 - No plots"
        return
    nOSTs = len(PlotsArray[0])
    fig = plt.figure(figsize=(nOSTs*10,nOSSs*10))
    fig.subplots_adjust(bottom=0.2)
    for OSSindex, OSSPlots in enumerate(PlotsArray):
        for OSTindex, OSTPlots in enumerate(OSSPlots):
            # Plot indices are 1-based and in row-major order
            subPlot = OSTindex+OSSindex*nOSTs + 1
            ax = fig.add_subplot(nOSSs, nOSTs, subPlot)
            offset = 0.0
            for p in OSTPlots:
                bar(ax, x, p['values'], width=width, offset=offset, color=p['color'], label=p['label'])
                offset += width
            ax.set_ylabel('Count')
            ax.set_xlabel(units)
            if not yMax is None:
                ax.set_ybound(lower=0, upper=yMax)
            ax.set_title('%s %s %s' % (p['oss'], p['ost'], name))
            ax.set_xticks(x+width)
            ax.set_xticklabels( bins, rotation=45, horizontalalignment='right' )
            ax.legend()
    if plot is None:
        #print "show array of %d by %d plots" % (nOSSs, nOSTs)
        plt.show()
    else:
        #print "save array of %d by %d plots to %s" % (nOSSs, nOSTs, plot)
        plt.savefig( name + '_' + plot )
    plt.cla()

#*******************************************************************************
