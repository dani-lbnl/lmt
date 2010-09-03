/*****************************************************************************
 *  Copyright (C) 2010 Lawrence Livermore National Security, LLC.
 *  This module written by Jim Garlick <garlick@llnl.gov>
 *  UCRL-CODE-232438
 *  All Rights Reserved.
 *
 *  This file is part of Lustre Monitoring Tool, version 2.
 *  Authors: H. Wartens, P. Spencer, N. O'Neill, J. Long, J. Garlick
 *  For details, see http://code.google.com/p/lmt/.
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License (as published by the
 *  Free Software Foundation) version 2, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE. See the terms and conditions of the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA or see
 *  <http://www.gnu.org/licenses/>.
 *****************************************************************************/

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <time.h>
#include <string.h>
#include <stdlib.h>

#include "list.h"
#include "util.h"
#include "sample.h"

struct sample_struct {
    double val[2];
    time_t time[2];
    int valid; /* count of valid samples [0,1,2] */
    int stale_secs;
};

sample_t
sample_create (int stale_secs)
{
    sample_t s = xmalloc (sizeof (*s));

    memset (s, 0, sizeof (*s));
    s->stale_secs = stale_secs;

    return s;
}

void
sample_destroy (sample_t s)
{
    free (s);
}

void
sample_invalidate (sample_t s)
{
    s->valid = 0;
}

void
sample_update (sample_t s, double val, time_t t)
{
    if (s->valid == 0) {
        s->time[1] = t;
        s->val[1] = val;
        s->valid++;
    } else if (s->time[1] < t) {
        s->time[0] = s->time[1];
        s->val[0] = s->val[1];
        s->time[1] = t;
        s->val[1] = val;
        if (s->valid < 2)
            s->valid++;
    }
}

double
sample_to_rate (sample_t s)
{
    double val = 0;

    if (s->valid == 2 && (time (NULL) - s->time[1]) <= s->stale_secs)
        val = (s->val[1] - s->val[0]) / (s->time[1] - s->time[0]);
    if (val < 0)
        val = 0;
    return val;
}

double
sample_to_val (sample_t s)
{
    if (s->valid > 0 && (time (NULL) - s->time[1]) <= s->stale_secs)
        return s->val[1];
    return 0;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
