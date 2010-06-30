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

typedef struct proc_ctx_struct *pctx_t;

pctx_t proc_create (const char *root);
void proc_destroy (pctx_t ctx);

int proc_open (pctx_t ctx, const char *path);

int proc_openf (pctx_t ctx, const char *fmt, ...)
                __attribute__ ((format (printf, 2, 3)));

void proc_close (pctx_t ctx);

int proc_scanf (pctx_t ctx, const char *path, const char *fmt, ...)
                __attribute__ ((format (scanf, 3, 4)));

int proc_gets (pctx_t ctx, const char *path, char *buf, int len);

int proc_eof (pctx_t ctx);

typedef enum {
    PROC_READDIR_NODIR = 1,
    PROC_READDIR_NOFILE = 2,
} proc_readdir_flag_t;
int proc_readdir (pctx_t ctx, proc_readdir_flag_t flag, char **namep);



/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */

