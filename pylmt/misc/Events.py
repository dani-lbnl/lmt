#!/usr/bin/env python
# events_plot.py
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
#
#   Use the data.py module to read in OST data and produce
# and plot of the transaction distribution

import sys
import os
import re
import time
import string
import argparse
import datetime
import numpy as np
import numpy.ma as ma
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import axes3d
from scipy import linspace, polyval, polyfit, sqrt, randn

#*******************************************************************************
# Begin class Events
class Events:
    """
    Gather a contiguous sequence of observations into an event. Develop
    a probability density function to describe the collection of events.
    An event consists of a sequence of contiguous observations all above
    a threshhold. The threshhold is calculated as the average observation
    over the course of the day.
    An event is a tuple:

    ost - The index of the ost on which the event occurred
    step - is the step number of the start of the event
    length - number of steps in the event
    size - number of bytes in the event (in MB)
    rate - the average value of the interior of events longer than 2 steps
    read - boolean saying if this is a read (otherwise it's a write)

    We don't really need the list of events since it is implicite in
    the OSTData objects. We want to construct a histogram of events, perhaps
    in two dimensions (size, length), and for that we need to know the range
    of possible vaues. A first sweep of the OSTData object can determine
    that and send it to the initializer.
    Revisiting this, I find that an early examination of the data shows
    fairly high daily counts up to around 20 GB then falling quickly to
    a sparse collection. Let's uniformly gather 1250 bins up to 25000 MB
    and then record a list of the events that are above that, including the
    largest (longest) ones.
    """
    def __init__(self, fs=None, min=0, max=25000, bins=1250, normed=False, log=False, threshhold=None):
        """
        I may change the Events histogram to have two dimensions at some
        point but for now it only has one - for the size of the event. Another
        embellisment would be to have the number of OSTs participating in
        the event.
        """
        self.fs = fs
        self.path = None
        self.bins = bins
        self.min = min
        self.max = max
        self.min_rate = 0.0
        self.max_rate = 500.0
        self.normed = normed
        self.log = log
        self.total = None
        self.average = None
        if fs != None:
            self.total = (fs.OSTReads.average*fs.OSTReads.count_all + fs.OSTWrites.average*fs.OSTWrites.count_all)/(1024*1024)
            self.average = self.total/(fs.OSTReads.count_all + fs.OSTWrites.count_all)
            self.read_total = (fs.OSTReads.average*fs.OSTReads.count_all)/(1024*1024)
            self.read_average = self.read_total/(fs.OSTReads.count_all)
            self.write_total = (fs.OSTWrites.average*fs.OSTWrites.count_all)/(1024*1024)
            self.write_average = self.write_total/(fs.OSTWrites.count_all)
        if threshhold == None:
            if fs == None:
                self.threshhold = 1.0
            else:
                #print "read average = %d" % fs.OSTReads.average
                #print "     count   = %d" % fs.OSTReads.count_all
                #print "write average = %d" % fs.OSTWrites.average
                #print "     count   = %d" % fs.OSTWrites.count_all
                self.threshhold = self.average
        else:
            self.threshhold = threshhold
        self.slope = None
        self.intercept = None
        self.r1 = None
        self.p1 = None
        self.stderr1 = None
        self.scale = None
        self.offset = None
        self.r2 = None
        self.p2 = None
        self.search_ost = None
        self.search_step = None
        self.search_length = None
        self.search_read = None
        self.search_sdev = None
        self.search_size = None
        self.read_events_total = 0
        self.read_events_steps = 0
        self.write_events_total = 0
        self.write_events_steps = 0
        self.stderr2 = None
        self.Histograms = np.zeros((self.bins, 3), dtype=int)
        self.Histograms[:,0] = int(self.min) + np.array(range(self.bins), dtype=int)*int((self.max - self.min)/self.bins)
        self.Sdevs = np.zeros((self.bins, 20), dtype=int)
        self.Lengths = np.zeros((self.bins, 5), dtype=int)
        self.Lengths[:,0] = np.array(range(self.bins), dtype=int)
        self.Rates = np.zeros((self.bins, 3), dtype=float)
        self.Rates[:,0] = self.min_rate + (np.array(range(self.bins), dtype=float)*(self.max_rate - self.min_rate))/float(self.bins)
        self.Zeros = np.zeros((self.bins, 3), dtype=int)
        self.Zeros[:,0] = np.array(range(self.bins), dtype=int)
        # events_count - The number of events with either:
        #         size > histogram cutoff (default 25,000 MB)
        #         length > bins
        #         zeros > bins
        # If any of those conditions pertains do not put the entry in
        # the histogram, but add it to the separately maintained list.
        # This all needs to be revisited, since I made big changes to
        # the actual event registration code.
        #   It takes two passes to get the Events array. The first pass
        # determines how many elements it will have. 'count' == True
        # means we just count them, and we'll fill in the array on a
        # later pass.
        self.events_count = 0
        self.events_index = 0
        if fs == None:
            # print "No FS"
            return
        #print "Now count the large and long events"
        self.events(count=True)
        #print "First pass identified %d events" % self.events_index
        self.events_count = self.events_index
        self.events_index = 0
        if self.events_count > 0:
            self.Events = np.zeros((self.events_count, 7), dtype=int)
            #print "%d long or large events" % self.events_count
        else:
            self.Events = None
            print "No long or large events recorded"
        self.Done = False

    def events(self, count = False, find = None):
        # Go through the read and the the write OST series looking for
        # non-zero events. Register them unless 'count' is True, in which
        # case just count them, since it's our first pass through the data.
        # If 'find' is not 'None' then don't register anything, just check
        # the search criteria.
        #print "Enter events loop"
        if (count == True) and (find != None):
            print "I am counting and finding at the same time"
        read = 1
        for ost in range(self.fs.OSTReads.num_osts - 1):
            size = 0.0
            length = 0
            zeros = 0
            for step in range(self.fs.OSTReads.num_steps - 1):
                (size, length, zeros) = self.register(ost, size, step, length, zeros, read, count, find)
            # for step in range(self.fs.OSTReads.num_steps - 1):
        # for ost in range(self.fs.OSTReads.num_osts - 1):
        read = 0
        for ost in range(self.fs.OSTWrites.num_osts - 1):
            size = 0.0
            length = 0
            zeros = 0
            for step in range(self.fs.OSTWrites.num_steps - 1):
                (size, length, zeros) = self.register(ost, size, step, length, zeros, read, count, find)
            # for step in range(self.fs.OSTWrites.num_steps - 1):
        # for ost in range(self.fs.OSTWrites.num_osts - 1):
        self.Done = True
        #print "Leave events loop"

    def register(self, ost, size, step, length, zeros, read, count, find):
        # This is the regular processing of the next step.
        # If we're just counting ('count' is True) then we won't actually
        # assign anything into the arrays. If we're just finding
        # ('find' != 'None') then check if the criteria are met.
        if read == 1:
            diffs = self.fs.OSTReads.diffs
            str = "read"
        else:
            diffs = self.fs.OSTWrites.diffs
            str = "write"
        # if read == 1:
        if (diffs[ost, step] is ma.masked) or (diffs[ost, step]/(1024*1024) < self.threshhold):
            # We have a "zero"
            if length == 0:
                # and it is in the midst of a series of zeros so note it and return
                zeros += 1
                return(0.0, 0, zeros)
            # if length == 0:
            # This zero terminates an event
            # print "event %d steps long (%s)" % (length, count)
            # If we've issued a "find" then do that and continue,
            # no need to accumulate or count things.
            if find != None:
                self.check_Event(ost, step, length, read, size, find)
                return(0.0, 0, 1)
            if size/(1024*1024) < self.min:
                print "OST %d has undersize %s event at step %d with %f < %f" % (ost, str, step, size/(1024*1024), self.min)
                sys.exit(1)
            if count == True:
                if read == 1:
                    self.read_events_total += size/(1024*1024)
                    self.read_events_steps += 1
                else:
                    self.write_events_total += size/(1024*1024)
                    self.write_events_steps += 1
            if size/(1024*1024) > self.max:
                # It is larger than we can put in Histogram, so it goes
                # in the Events array (or we just count it if 'count' is True)
                # and return
                #print "Register size event"
                self.register_Event(ost, size, step, length, zeros, read, count)
                return(0.0, 0, 1)
            # if size/(1024*1024) > self.max:
            # The check for length < self.bins is next, since otherwise
            # we're double counting the entry.
            if length < self.bins:
                if count == False:
                    self.Lengths[length,2-read] += 1
                    self.Lengths[length,4-read] += size
            else:
                #print "Register length event"
                self.register_Event(ost, size, step, length, zeros, read, count)
                return(0.0, 0, 1)
            # if length < self.bins:
            # It fits in Histogram, so find the bin
            bin = int((size/(1024*1024) - self.min)*self.bins/(self.max - self.min))
            if bin == self.bins:
                bin -= 1
            if bin > self.bins:
                print "OST %d has an out of range %s bin (%d) at step %d with %f > %f" % (ost, str, bin, step, size/(1024*1024), self.max)
                sys.exit(1)
            if count == False:
                # 2-read means reads are in column 1 and writes in column 2
                self.Histograms[bin, 2-read] += 1
            # And put it in Rates if it is longer than some minimum number of observations
            if length > 4:
                # find the bin
                bin = int((size/(length*5*1024*1024) - self.min_rate)*self.bins/(self.max_rate - self.min_rate))
                # print "bin = %d, size = %f, length = %d" % (bin, size, length)
                if bin >= self.bins:
                    # There is a very rare occasion when the rate is above the max
                    bin = self.bins - 1
                if count == False:
                    # 2-read means reads are in column 1 and writes in column 2
                    self.Rates[bin, 2-read] += 1
            # Calculate the ratio of the standard deviation of the values in the event
            # to their average. If the value is below some small fraction of 1 then the
            # event probably looks like a square wave pulse.
            # The interior of the event is from step - length + 1 to step - 2 and only exists
            # if length > 2.But keep in mind the the endpoint of the range is not included
            # in Pythonese.
            # N.B. this means we'll never do a 'find' for length <= 2.
            if length <= 2:
                return(0.0, 0, 1)
            #print diffs[ost, step-length+1:step-1]
            ave = diffs[ost, step-length+1:step-1].sum()/(length-2)
            #print "ost=%d, step=%d" % (ost, step)
            sdev = np.std(diffs[ost, step-length+1:step-1])
            #print "len = %d, ave = %d, sdev = %f" % (length, ave, sdev)
            # Cut the histogram into 10 slices. We're're really only interested
            # in cases where the std is small compared to the ave. Longer events
            # with low sdev/ave are especially good candidates for being simple
            # square wave pulses.
            bin = int((float(sdev)/float(ave))*10)
            #print "bin = %d" % bin
            if bin >= 10:
                bin = 9
            if bin < 0:
                bin = 0
            if count == False:
                self.Sdevs[length, bin + 10*(1-read)] += 1
            # We're done with the case that we saw a zero so just return
            return(0.0, 0, 1)
        # if diffs[ost, step]/(1024*1024) < self.threshhold:
        # We have (or are in) an event
        if zeros > 0:
            # It terminates a run of zeros
            # print "                       zeros event %d steps long (%s)" % (zeros, count)
            if zeros < self.bins:
                if count == False:
                    self.Zeros[zeros,2-read] += 1
            else:
                # We have to add it to the Events array
                #print "Register zeros event"
                self.register_Event(ost, size, step, length, zeros, read, count)
            # if zeros < self.bins:
        # if zeros > 0:
        size += diffs[ost, step]
        length += 1
        return(size, length, 0)

    def register_Event(self, ost, size, step, length, zeros, read, count):
        # this is the special purpose code that puts an outsized (or long)
        # event into a special record. N.B. If this is a zeros event then
        # length == 0 and the zeros parameter has the actual length.
        if count == False:
            if self.events_index >= self.events_count:
                print "we ran out of space (events_index=%d) for large events in the events array: (ost=%d, step=%d, length=%d, size=%f,rate=.., read=0 " % (self.events_index, ost, step, length, size/(1024*1024))
                sys.exit(1)
            # if self.events_index >= self.events_count:
        if size is ma.masked:
            print "size is masked"
            self.events_index += 1
            return
        if np.isnan(size):
            print "size is a nan"
            self.events_index += 1
            return
        # The values we're trying to subtract may not exist
        # read == 0 if it's writes
        ave = 0
        sdev = 0
        edges = 0
        if zeros == 0:
            # It's a regular non-zeros events
            if read == 1:
                diffs = self.fs.OSTReads.diffs
            else:
                diffs = self.fs.OSTWrites.diffs
            # if read == 1:
            try:
                if diffs[ost, step - length] is not ma.masked:
                    edges = diffs[ost, step - length]
                    try:
                        edges +=  diffs[ost, step - 1]
                    except:
                        edges += 0
                    # try:
                #if diffs[ost, step - length] is not ma.masked:
            except:
                if diffs[ost, step - 1] is not ma.masked:
                    try:
                        edges = diffs[ost, step - 1]
                    except:
                        edges = 0
                    # try:
                #if diffs[ost, step - 1] is not ma.masked:
            #try
            if length > 2:
                #print "ost = %d" % ost
                #print "step = %d" % step
                #print "length = %d" % length
                #print "size = %d" % size
                #print "edges = %d" % edges
                ave = int((size - edges)/((length - 2)*1024*1024))
                sdev = int(np.std(diffs[ost, step-length+1:step-1])/(1024*1024))
            # if length > 2:
        else:
            length = zeros
        # if zeros == 0:
        # Put (ost, step, length, MB, ave, sdev, read) in Events array
        sdev_r = 0.0
        if ave > 0:
            sdev_r = float(sdev)/float(ave)
        #print "(%d, %d, %d, %d, %d, %f, %f, %f, %d)" % (self.events_count, self.events_index, ost, step - length, length, float(size)/(1024*1024), float(ave)/(1024*1024), sdev_r, read)
        if count == False:
            self.Events[self.events_index,:] = (ost, step - length, length, int(size/(1024*1024)), ave, sdev, read)
        self.events_index += 1
        # print self.events_index

    def check_Event(self, ost, step, length, read, size, find):
        found = True
        for key in find:
            if find[key] != None:
                if (key == 'ost'):
                    if (find[key] != ost):
                        found = False
                elif (key == 'step'):
                    if (find[key] != step):
                        found = False
                elif (key == 'length'):
                    if (find[key] != length):
                        found = False
                elif (key == 'read'):
                    if (find[key] != read):
                        found = False
                elif (key == 'size'):
                    if (find[key] != size):
                        found = False
                elif (key == 'minStep'):
                    if (find[key] > step):
                        found = False
                elif (key == 'maxStep'):
                    if (find[key] < step):
                        found = False
                elif (key == 'minLength'):
                    if (find[key] > length):
                        found = False
                elif (key == 'maxLength'):
                    if (find[key] < length):
                        found = False
                elif (key == 'minSize'):
                    if (find[key] > size):
                        found = False
                elif (key == 'maxSize'):
                    if (find[key] < size):
                        found = False
                else:
                    print "Unrecognized key %s with value %s" %(key, str(find[key]))
                    return
            # if find[key] != None:
        # for key in find:
        if found == True:
            print "%d\t%d\t%d\t%d\t%d" % (ost, step, length, read, size)

    def show_Event(self):
        if (self.search_ost == None) or (self.search_step == None) or (self.search_length == None) or (self.search_read == None) or (self.search_sdev == None):
            print "No event found"
            if self.search_ost != None:
                print "ost =", self.search_ost
            if self.search_step != None:
                print "step =", self_search_step
            if self.search_length != None:
                print "length =", self_search_length
            if self.search_read != None:
                print "read =", self_search_read
            if self.search_sdev != None:
                print "sdev =", self_search_sdev
            sys.exit(0)
        if self.search_read == 1:
            diffs = self.fs.OSTReads.diffs
        else:
            diffs = self.fs.OSTWrites.diffs
        print "ost = %d, start = %d, length = %d, read = %d, sdev/ave = %f" % (self.search_ost, self.search_step - self.search_length, self.search_length, self.search_read, self.search_sdev)
        print diffs[self.search_ost,self.search_step-self.search_length:self.search_step]

    def plot_Event(self, plot=None, fig = None, xbound = None, ybound = None, title = None, read=False, write=False):
        if (self.search_ost == None) or (self.search_step == None) or (self.search_length == None) or (self.search_read == None) or (self.search_sdev == None):
            print "No event found"
            if self.search_ost != None:
                print "ost =", self.search_ost
            if self.search_step != None:
                print "step =", self_search_step
            if self.search_length != None:
                print "length =", self_search_length
            if self.search_read != None:
                print "read =", self_search_read
            if self.search_sdev != None:
                print "sdev =", self_search_sdev
            sys.exit(0)
        if self.search_read == True:
            diffs = self.fs.OSTReads.diffs[self.search_ost,:]
        else:
            diffs = self.fs.OSTWrites.diffs[self.search_ost,:]
        start = self.search_step - self.search_length - 1
        if start < 0:
            start = 0
        end = self.search_step + 1
        if end > len(diffs):
            end = len(diffs)
        while (start > 0) and (diffs[start - 1]/(1024*1024) < self.threshhold):
            start -= 1
        while  (end < len(diffs)) and (diffs[end]/(1024*1024) < self.threshhold):
            end += 1
        if fig == None:
            fig = plt.figure()
        if read == True:
            read_event = self.fs.OSTReads.diffs[self.search_ost,start:end]/(1024.0*1024.0*5.0)
            X = (np.array(range(len(read_event))) + self.search_step - self.search_length)*5
            plt.plot(X, read_event, 'r-', label='read')
        if write == True:
            write_event = self.fs.OSTWrites.diffs[self.search_ost,start:end]/(1024.0*1024.0*5.0)
            X = (np.array(range(len(write_event))) + self.search_step - self.search_length)*5
            plt.plot(X, write_event, 'b-', label='write')
        ax, = fig.get_axes()
        if xbound != None:
            ax.set_xbound(upper = xbound)
        if ybound != None:
            ax.set_ybound(lower = 0.0, upper = ybound)
        if title != None:
            plt.title(title)
        plt.ylabel("MB/s")
        plt.xlabel("seconds")
        plt.legend()
        if plot == None:
            plt.show()
            plt.close()
        else:
            if plot != 'wait':
                plt.savefig(plot)
        return(fig)

    def aves(self):
        # Each self.Histogram bin is 20 MB in the default arrangement
        # So when we find the centroid of the power spectrum the average
        # amount of data moved in an event is 20 MB times that index.
        # For now this ignores the Events array.
        power = self.Histograms[:,0]*self.Histograms[:,1]
        power_cs = np.cumsum(power)
        total = power.sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "The average read size = %d MB" % (index*20)
        reads = np.where(self.Events[:,6] == 1)
        total += self.Events[reads,3].sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "                     or %d MB if you include the biggest ones" % (index*20)
        power = self.Histograms[:,0]*self.Histograms[:,2]
        power_cs = np.cumsum(power)
        total = power.sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "the average write size = %d MB" % (index*20)
        writes = np.where(self.Events[:,6] == 0)
        total += self.Events[writes,3].sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "                      or %d MB if you include the biggest ones" % (index*20)
        # Each self.Lengths bin represents one step so the index of
        # of the centroid is the average length
        power = self.Lengths[:,0]*self.Lengths[:,1]
        power_cs = np.cumsum(power)
        total = power.sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "the average read length = %d seconds" % (index*5)
        reads = np.where(np.logical_and((self.Events[:,6] == 1), (self.Events[:,3] > 0)))
        total += self.Events[reads,2].sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "                       or %d seconds if you include the biggest ones" % (index*20)
        power = self.Lengths[:,0]*self.Lengths[:,2]
        power_cs = np.cumsum(power)
        total = power.sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "the average write length = %d seconds" % (index*5)
        writes = np.where(np.logical_and((self.Events[:,6] == 0), (self.Events[:,3] > 0)))
        total += self.Events[writes,2].sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "                       or %d seconds if you include the biggest ones" % (index*20)
        power = self.Zeros[:,0]*self.Zeros[:,1]
        power_cs = np.cumsum(power)
        total = power.sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "the average read zeros length = %d seconds" % (index*5)
        reads = np.where(np.logical_and((self.Events[:,6] == 1), (self.Events[:,3] == 0)))
        total += self.Events[reads,2].sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "                       or %d seconds if you include the biggest ones" % (index*20)
        power = self.Zeros[:,0]*self.Zeros[:,2]
        power_cs = np.cumsum(power)
        total = power.sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "the average write zeros length = %d seconds" % (index*5)
        writes = np.where(np.logical_and((self.Events[:,6] == 0), (self.Events[:,3] == 0)))
        total += self.Events[writes,2].sum()
        lesser = np.where(power_cs < total/2)
        index = len(power_cs[lesser])
        print "                        or %d seconds if you include the biggest ones" % (index*20)

    def append(self, append):
        """
        Once you start concatenating events array, there is no more room in them
        for additional events. There shouldn't have been any extra room in the
        first place.
        """
        self.Histograms[:,1] += append.Histograms[:,1]
        self.Histograms[:,2] += append.Histograms[:,2]
        self.Sdevs += append.Sdevs
        self.Lengths[:,1] += append.Lengths[:,1]
        self.Lengths[:,2] += append.Lengths[:,2]
        self.Lengths[:,3] += append.Lengths[:,3]
        self.Lengths[:,4] += append.Lengths[:,4]
        self.Rates[:,1] += append.Rates[:,1]
        self.Rates[:,2] += append.Rates[:,2]
        self.Zeros[:,1] += append.Zeros[:,1]
        self.Zeros[:,2] += append.Zeros[:,2]
        # One or the other might have no events
        if (self.events_count == 0) or (self.Events == None):
            self.Events = append.Events
            self.events_count = append.events_count
        else:
            if (append.events_count > 0) and (append.Events != None):
                self.Events = np.concatenate((self.Events,append.Events), axis = 0)
                self.events_count += append.events_count

    def load(self, hist = None, events = None, lengths=None, rates=None, zeros=None, sdevs=None):
        """
        """
        if hist == None:
            hist = "events_hist.data"
        if events == None:
            events = "events_list.data"
        if lengths == None:
            lengths = "lengths_hist.data"
        if rates == None:
            rates = "rates_hist.data"
        if zeros == None:
            zeros = "zeros_hist.data"
        if sdevs == None:
            sdevs = "sdevs_hist.data"
        self.Histograms = np.loadtxt(hist)
        try:
            Events = np.loadtxt(events)
            if np.ndim(Events) == 1:
                self.events_count = 1
                self.Events = np.zeros((self.events_count, 7), dtype=int)
                self.Events[0,:] = Events
            else:
                self.Events = Events
                self.events_count = len(self.Events[:,0])
            # if np.ndim(Events) == 1:
        except IOError:
            self.Events = None
            self.events_count = 0
        self.Lengths = np.loadtxt(lengths)
        self.Rates = np.loadtxt(rates)
        self.Zeros = np.loadtxt(zeros)
        self.Sdevs = np.loadtxt(sdevs)
        # The first bin is the minimum left edge
        self.min = self.Histograms[0,0]
        # but the last bin is left edge,too, so you have to add a bin width to get the max
        self.max = self.Histograms[1,0] + self.Histograms[-1,0]
        self.Done = True

    def save(self, hist=None, events=None, lengths=None, rates=None, zeros=None, sdevs=None):
        """
        """
        if hist == None:
            hist = "events_hist.data"
        if events == None:
            events = "events_list.data"
        if lengths == None:
            lengths = "lengths_hist.data"
        if rates == None:
            rates = "rates_hist.data"
        if zeros == None:
            zeros = "zeros_hist.data"
        if sdevs == None:
            sdevs = "sdevs_hist.data"
        np.savetxt(hist, self.Histograms, fmt='%d\t%d\t%d')
        try:
            np.savetxt(events, self.Events, fmt='%d\t%d\t%d\t%d\t%d\t%d\t%d')
        except IndexError:
            print "saved empty events file"
        np.savetxt(lengths, self.Lengths, fmt='%d\t%d\t%d\t%d\t%d')
        np.savetxt(rates, self.Rates, fmt='%f\t%f\t%f')
        np.savetxt(zeros, self.Zeros, fmt='%d\t%d\t%d')
        np.savetxt(sdevs, self.Sdevs, fmt='%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d')

    def show_threshhold(self):
        print "threshhold = %f" % self.threshhold
        print "read average = %f" % self.read_average
        print "write average = %f" % self.write_average

    def show_fraction(self):
        #print "total = %f" % self.events_total
        #print "threshhold = %f" % self.threshhold
        #print " read count = %d" % self.fs.OSTReads.count_all
        #print "write count = %d" % self.fs.OSTWrites.count_all
        #print " read nonmasked = %d" % self.fs.OSTReads.count_nonmasked
        #print "write nonmaksed = %d" % self.fs.OSTWrites.count_nonmasked
        if self.total == None:
            print "No info on fraction"
            return
        print "fraction of read I/O in read events = (%f MB)/(%f MB) = %f" % (self.read_events_total, self.read_total, self.read_events_total/self.read_total)
        print "fraction of steps participating in read events = %d/%d = %f" % (self.read_events_steps, self.fs.OSTReads.count_all, float(self.read_events_steps)/float(self.fs.OSTReads.count_all))
        print "fraction of write I/O in write events = (%f MB)/(%f MB) = %f" % (self.write_events_total, self.write_total, self.write_events_total/self.write_total)
        print "fraction of steps participating in write events = %d/%d = %f" % (self.write_events_steps, self.fs.OSTWrites.count_all, float(self.write_events_steps)/float(self.fs.OSTWrites.count_all))

    def plot_hist(self, plot=None, fig = None, xbound = None, ybound = None, title = None, read=False, write=False):
        """
        This routine does not yet account for the events array
        """
        scale=2
        if fig == None:
            fig = plt.figure(figsize=(scale*8,scale*6))
        if self.Done == False:
            print "Events.plot(): The Events histograms have not been calculated"
            return
        #model = np.log(self.Histograms[:,0])/np.log(self.Histograms[:,1])
        if self.log == False:
#            plt.plot(self.Histograms[:,0], model, 'r-', label='read')
            if read == True:
                plt.plot(self.Histograms[:,0], self.Histograms[:,1], 'r-', label='read', linewidth=scale*2)
            if write == True:
                plt.plot(self.Histograms[:,0], self.Histograms[:,2], 'b-', label='write', linewidth=scale*2)
        else:
#            plt.semilogy(self.Histograms[:,0], model, 'r-', label='read')
            if read == True:
                plt.semilogy(self.Histograms[:,0], self.Histograms[:,1], 'r-', label='read', linewidth=scale*2)
            if write == True:
                plt.semilogy(self.Histograms[:,0], self.Histograms[:,2], 'b-', label='write', linewidth=scale*2)
        ax, = fig.get_axes()
        if xbound != None:
            ax.set_xbound(upper = xbound)
        if ybound != None:
            if self.log == True:
                ax.set_ybound(lower = 0.5, upper = ybound)
            else:
                ax.set_ybound(lower = -0.5, upper = ybound)
        if title != None:
            plt.title(title, size=scale*18)
        plt.tick_params(axis='both', labelsize=scale*12)
        plt.ylabel("Count", size=scale*16)
        plt.xlabel("MB", size=scale*16)
        plt.legend()
        if plot == None:
            plt.show()
            plt.clear()
        else:
            if plot != 'wait':
                plt.savefig(plot)
        return(fig)

    def plot_lengths(self, plot=None, fig = None, xbound = None, ybound = None, title = None, read=False, write=False):
        """
        This routine does not yet account for the events array
        """
        scale=2
        if fig == None:
            fig = plt.figure(figsize=(scale*8,scale*6))
        if self.Done == False:
            print "Events.plot(): The Events histograms have not been calculated"
            return
        if self.log == False:
            if read == True:
                plt.plot(self.Lengths[:,0], self.Lengths[:,1], 'r-', label='read', linewidth=scale*2)
            if write == True:
                plt.plot(self.Lengths[:,0], self.Lengths[:,2], 'b-', label='write', linewidth=scale*2)
        else:
            if read == True:
                plt.semilogy(self.Lengths[:,0], self.Lengths[:,1], 'r-', label='read', linewidth=scale*2)
            if write == True:
                plt.semilogy(self.Lengths[:,0], self.Lengths[:,2], 'b-', label='write', linewidth=scale*2)
        ax, = fig.get_axes()
        if xbound != None:
            ax.set_xbound(upper = xbound)
        if ybound != None:
            if self.log == True:
                ax.set_ybound(lower = 0.5, upper = ybound)
            else:
                ax.set_ybound(upper = ybound)
        if title != None:
            plt.title(title, size=scale*18)
        plt.tick_params(axis='both', labelsize=scale*12)
        plt.ylabel("Count", size=scale*16)
        plt.xlabel("steps (5 sec/step)", size=scale*16)
        plt.legend()
        if plot == None:
            plt.show()
        else:
            plt.savefig(plot)
        return(fig)

    def plot_lengths_weight(self, plot=None, fig = None, xbound = None, ybound = None, title = None, read=False, write=False):
        """
        This routine does not yet account for the events array
        """
        if fig == None:
            fig = plt.figure()
        if self.Done == False:
            print "Events.plot(): The Events histograms have not been calculated"
            return
        if self.log == False:
            if read == True:
                plt.plot(self.Lengths[:,0], self.Lengths[:,3], 'r-', label='read')
            if write == True:
                plt.plot(self.Lengths[:,0], self.Lengths[:,4], 'b-', label='write')
        else:
            if read == True:
                plt.semilogy(self.Lengths[:,0], self.Lengths[:,3], 'r-', label='read')
            if write == True:
                plt.semilogy(self.Lengths[:,0], self.Lengths[:,4], 'b-', label='write')
        ax, = fig.get_axes()
        if xbound != None:
            ax.set_xbound(upper = xbound)
        if ybound != None:
            if self.log == True:
                ax.set_ybound(lower = 0.5, upper = ybound)
            else:
                ax.set_ybound(upper = ybound)
        if title != None:
            plt.title(title)
        plt.ylabel("bytes")
        plt.xlabel("steps (5 sec/step)")
        plt.legend()
        if plot == None:
            plt.show()
        else:
            plt.savefig(plot)
        return(fig)

    def plot_lengths_rate(self, plot=None, fig = None, xbound = None, ybound = None, title = None, read=False, write=False):
        """
        This routine does not yet account for the events array
        """
        scale=2
        if fig == None:
            fig = plt.figure(figsize=(scale*8,scale*6))
        if self.Done == False:
            print "Events.plot(): The Events histograms have not been calculated"
            return
        if read == True:
            rate = np.array(self.Lengths[:,3], dtype=float)
            x = np.array(self.Lengths[:,0], dtype=float)
            rate = rate/(5.0*x*np.array(self.Lengths[:,1], dtype=float))
            rate[np.where(np.isinf(rate))] = 0.0
            plt.plot(x, rate/(1024*1024), 'r-', label='read', linewidth=scale*2)
        if write == True:
            rate = np.array(self.Lengths[:,4], dtype=float)
            x = np.array(self.Lengths[:,0], dtype=float)
            rate = rate/(5.0*x*np.array(self.Lengths[:,2], dtype=float))
            rate[np.where(np.isinf(rate))] = 0.0
            plt.plot(x, rate/(1024*1024), 'b-', label='write', linewidth=scale*2)
        ax, = fig.get_axes()
        if xbound != None:
            ax.set_xbound(upper = xbound)
        if ybound != None:
            ax.set_ybound(upper = ybound)
        if title != None:
            plt.title(title, size=scale*18)
        plt.tick_params(axis='both', labelsize=scale*12)
        plt.ylabel("MB/s", size=scale*16)
        plt.xlabel("steps (5 sec/step)", size=scale*16)
        plt.legend()
        if plot == None:
            plt.show()
        else:
            plt.savefig(plot)
        return(fig)

    def plot_rates(self, plot=None, fig = None, xbound = None, ybound = None, title = None, read=False, write=False):
        """
        This routine does not yet account for the events array
        """
        scale=2
        if fig == None:
            fig = plt.figure(figsize=(scale*8,scale*6))
        if self.Done == False:
            print "Events.plot(): The Events histograms have not been calculated"
            return
        if self.log == False:
            if read == True:
                plt.plot(self.Rates[:,0], self.Rates[:,1], 'r-', label='read', linewidth=scale*2)
            if write == True:
                plt.plot(self.Rates[:,0], self.Rates[:,2], 'b-', label='write', linewidth=scale*2)
        else:
            if read == True:
                plt.semilogy(self.Rates[:,0], self.Rates[:,1], 'r-', label='read', linewidth=scale*2)
            if write == True:
                plt.semilogy(self.Rates[:,0], self.Rates[:,2], 'b-', label='write', linewidth=scale*2)
        ax, = fig.get_axes()
        if xbound != None:
            ax.set_xbound(upper = xbound)
        if ybound != None:
            if self.log == True:
                ax.set_ybound(lower = 0.5, upper = ybound)
            else:
                ax.set_ybound(upper = ybound)
        if title != None:
            plt.title(title, size=scale*18)
        plt.tick_params(axis='both', labelsize=scale*12)
        plt.ylabel("Count", size=scale*16)
        plt.xlabel("MB/s", size=scale*16)
        plt.legend()
        if plot == None:
            plt.show()
        else:
            plt.savefig(plot)
        return(fig)

    def plot_zeros(self, plot=None, fig = None, xbound = None, ybound = None, title = None, read=False, write=False):
        """
        This routine does not yet account for the events array
        """
        if fig == None:
            fig = plt.figure()
        if self.Done == False:
            print "Events.plot(): The Events histograms have not been calculated"
            return
        if self.log == False:
            if read == True:
                plt.plot(self.Zeros[:,0], self.Zeros[:,1], 'r-', label='read')
            if write == True:
                plt.plot(self.Zeros[:,0], self.Zeros[:,2], 'b-', label='write')
        else:
            if read == True:
                plt.semilogy(self.Zeros[:,0], self.Zeros[:,1], 'r-', label='read')
            if write == True:
                plt.semilogy(self.Zeros[:,0], self.Zeros[:,2], 'b-', label='write')
        ax, = fig.get_axes()
        if xbound != None:
            ax.set_xbound(upper = xbound)
        if ybound != None:
            if self.log == True:
                ax.set_ybound(lower = 0.5, upper = ybound)
            else:
                ax.set_ybound(upper = ybound)
        if title != None:
            plt.title(title)
        plt.ylabel("Count")
        plt.xlabel("steps (5 sec/step)")
        plt.legend()
        if plot == None:
            plt.show()
        else:
            plt.savefig(plot)
        return(fig)

    def plot_list(self, plot=None, fig = None, xbound = None, ybound = None, title = None, read=False, write=False):
        """
        This routine does not yet account for the events array
        """
        if self.Events == None:
            print "There were no large or long events"
            return
        if fig == None:
            fig = plt.figure()
        if self.Done == False:
            print "Events.plot(): The Events histograms have not been calculated"
            return
        max = np.max(self.Events[:,3])
        reads = self.Events[:,6] == 1
        read_list = self.Events[np.where(reads), 3]
        read_hist, bins = np.histogram(read_list, bins=self.bins, range=(0, max))
        writes = self.Events[:,6] == 0
        write_list = self.Events[np.where(writes), 3]
        write_hist, bins = np.histogram(write_list, bins=self.bins, range=(0, max))
        if self.log == False:
            plt.plot(bins[1:-1], read_hist[1:], 'r-', label='read')
            plt.plot(bins[1:-1], write_hist[1:], 'b-', label='write')
        else:
            plt.semilogy(bins[1:-1], read_hist[1:], 'r-', label='read')
            plt.semilogy(bins[1:-1], write_hist[1:], 'b-', label='write')
        ax, = fig.get_axes()
        if xbound != None:
            ax.set_xbound(upper = xbound)
        if ybound != None:
            if self.log == True:
                ax.set_ybound(upper = ybound, lower=0.5)
            else:
                ax.set_ybound(upper = ybound)
        if title != None:
            plt.title(title)
        plt.ylabel("Count")
        plt.xlabel("MB")
        plt.legend()
        if plot == None:
            plt.show()
        else:
            if plot != 'wait':
                plt.savefig(plot)
        return(fig)

    def successfull_plot_sdevs(self, plot=None, fig = None, xbound = None, ybound = None, title = None, read=False, write=False):
        """
        This is judt me experimenting
        """
        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        X, Y, Z = axes3d.get_test_data(0.05)
        ax.plot_wireframe(X, Y, Z, rstride=10, cstride=10)
        print X
        print Y
        print Z
        plt.show()


    def plot_sdevs(self, plot=None, fig = None, xbound = None, ybound = None, zbound = None, title = None, read=False, write=False):
        """
        This routine does not yet account for the events array
        """
        if self.Done == False:
            print "Events.plot_sdev(): The Events standard deviation histograms have not been calculated"
            return
        if fig == None:
            fig = plt.figure()
        if xbound == None:
            xbound = self.bins
        if read == True:
            Z = self.Sdevs[0:xbound,0:10]
        else:
            Z = self.Sdevs[0:xbound,10:20]
        extent = [0, 1.0, 0, xbound]
        if zbound != None:
            Z_max = np.ones(self.bins, 10)*zbound
            plt.contour(np.minimum(Z, Z_max), extent=extent, aspect='auto', levels=[0,1,3,10,30,100,300,1000,3000,10000,30000,100000], colors=[(0.5,0.5,0.5),(0.75,0.75,0.75),(1.0,1.0,1.0),(0.5,0.0,0.0),(0.75,0.0,0.0),(1.0,0.0,0.0),(0.0,0.5,0.0),(0.0,0.75,0.0),(0.0,1.0,0.0),(0.0,0.0,0.5),(0.0,0.0,0.75),(0.0,0.0,1.0)])
        else:
            plt.contour(Z, extent=extent, aspect='auto', levels=[0,1,3,10,30,100,300,1000,3000,10000,30000,100000], colors=[(0.5,0.5,0.5),(0.75,0.75,0.75),(1.0,1.0,1.0),(0.5,0.0,0.0),(0.75,0.0,0.0),(1.0,0.0,0.0),(0.0,0.5,0.0),(0.0,0.75,0.0),(0.0,1.0,0.0),(0.0,0.0,0.5),(0.0,0.0,0.75),(0.0,0.0,1.0)])
        if title != None:
            plt.title(title)
        ax = fig.get_axes()
        ax[0].set_ylabel("length")
        ax[0].set_xlabel("sdev/ave")
        #plt.legend()
        if plot == None:
            plt.show()
#            plt.clear()
        else:
            if plot != 'wait':
                plt.savefig(plot)
        return(fig)

# End of class Events
#******************************************************************************

if __name__ == "__main__":
    """
Events.py <opts>
Options include:
-d <data>   Path to directory of previously calculated data to load
             rather than loading ost data
-e          Produce a histogram of events
-E          Produce an extended histogram of events above the high cutoff
-f <fs>     The file system (default <scratch>)
-h          A help message
-k          Produce a graph of average achieved rate versus event length
-l          Use a log scale
-L          Plot the histogram of lengths
-p          Send any plots to files rather than the screen
-r          Include read data in the plot
-t <title>  Put 'title' in the title of the graph
-V          Print the version and exit
-w          Include write data in the plot
-W          Produce a weighted histogram showing the amount of I/O at each event length
-x <xbound> x-axis bounds
-y <ybound> y-axis bounds
-z <zbound> z-axis bounds (do I use this?)
-Z          Produce a histogram of the duration of inactivity between events

Read in previously caluculated event data and plot a graph(s).
    """
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-d', '--data', default=".", type=str, help='Directory with previously extracted OST data (default ".")')
    parser.add_argument('-e', '--hist', action='store_true', default=False, help='plot the events histogram below the cutoff (default 25,000 MB)')
    parser.add_argument('-E', '--events', action='store_true', default=False, help='plot the events above the cutoff (default 25,000 MB)')
    parser.add_argument('-f', '--fs', default="scratch", type=str, help='The name of the file system whose data is in "data"')
    parser.add_argument('-k', '--lrate', action='store_true', default=False, help='plot average rate versus length')
    parser.add_argument('-l', '--log', action='store_true', default=False, help='plot with a log-scaled y-axis')
    parser.add_argument('-L', '--lengths', action='store_true', default=False, help='plot the histogram of event lengths below the cutoff (default 1250 steps)')
    parser.add_argument('-p', '--plot', action='store_true', default=False, help='send the plot(s) to a file')
    parser.add_argument('-r', '--read', action='store_true', default=False, help='Plot read distribution')
    parser.add_argument('-R', '--rates', action='store_true', default=False, help='plot the histogram of event rates below the system maximum (default 500 MB/s)')
    parser.add_argument('-s', '--sdevs', action='store_true', default=False, help='plot the histogram standard deviations below the cutoff (default 1250 steps)')
    parser.add_argument('-t', '--title', default=None, type=str, help='Title for the graph(s)')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.1')
    parser.add_argument('-w', '--write', action='store_true', default=False, help='Plot write distribution')
    parser.add_argument('-W', '--weight', action='store_true', default=False, help='Plot weighted distribution over lengths')
    parser.add_argument('-x', '--xbound', default=None, type=float, help='Set the x-axis upper bound to this value')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    parser.add_argument('-z', '--zbound', default=None, type=float, help='Set the z-axis upper bound to this value')
    parser.add_argument('-Z', '--zeros', action='store_true', default=False, help='plot the histogram of zeros below the cutoff (default 1250 steps)')
    args = parser.parse_args()
    if (args.read == False) and (args.write == False):
        print "You need to specify at least read (-r) or write (-w)"
        sys.exit(1)
    E = Events(log=args.log)
    hist = args.data + "/events_hist.data"
    events = args.data + "/events_list.data"
    lengths = args.data + "/lengths_hist.data"
    rates = args.data + "/rates_hist.data"
    zeros = args.data + "/zeros_hist.data"
    sdevs = args.data + "/sdevs_hist.data"
    for file in [hist, events, lengths, rates, zeros, sdevs]:
        if not os.access(file, os.F_OK):
            print 'Missing file ', file, 'in', args.data
            sys.exit(1)
    E.load(hist, events, lengths, rates, zeros, sdevs)
    if args.hist == True:
        fig = E.plot_hist(plot='wait', xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
        if args.plot == True:
            plt.savefig(args.data + "/events_hist.png")
        else:
            plt.show()
    if args.events == True:
        if args.plot == True:
            E.plot_list(args.data + "/events_list.png", xbound=args.xbound, ybound=args.ybound, title=args.title)
        else:
            E.plot_list(xbound=args.xbound, ybound=args.ybound, title=args.title)
    if args.lengths == True:
        if args.plot == True:
            E.plot_lengths(args.data + "/lengths_hist.png", xbound=args.xbound, ybound=args.ybound, title=args.title)
        else:
            E.plot_lengths(xbound=args.xbound, ybound=args.ybound, title=args.title)
    if args.rates == True:
        if args.plot == True:
            E.plot_rates(args.data + "/rates_hist.png", xbound=args.xbound, ybound=args.ybound, title=args.title)
        else:
            E.plot_rates(xbound=args.xbound, ybound=args.ybound, title=args.title)
    if args.zeros == True:
        if args.plot == True:
            E.plot_zeros(args.data + "/zeros_hist.png", xbound=args.xbound, ybound=args.ybound, title=args.title)
        else:
            E.plot_zeros(xbound=args.xbound, ybound=args.ybound, title=args.title)
    if args.sdevs == True:
        if args.plot == True:
            E.plot_sdevs(args.data + "/sdevs_hist.png", xbound=args.xbound, ybound=args.ybound, zbound=args.zbound, title=args.title, read=args.read, write=args.write)
        else:
            E.plot_sdevs(xbound=args.xbound, ybound=args.ybound, zbound=args.zbound, title=args.title, read=args.read, write=args.write)
    if args.weight == True:
        if args.plot == True:
            E.plot_lengths_weight(args.data + "/lengths_weight_hist.png", xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
        else:
            E.plot_lengths_weight(xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
    if args.lrate == True:
        if args.plot == True:
            E.plot_lengths_rate(args.data + "/lengths_rates_hist.png", xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)
        else:
            E.plot_lengths_rate(xbound=args.xbound, ybound=args.ybound, title=args.title, read=args.read, write=args.write)

