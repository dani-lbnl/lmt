/*****************************************************************************
 *  Copyright (C) 2007-2010 Lawrence Livermore National Security, LLC.
 *  This module written by Jim Garlick <garlick@llnl.gov>.
 *  UCRL-CODE-232438
 *  All Rights Reserved.
 *
 *  This file is part of Lustre Monitoring Tool, version 2.
 *  Authors: H. Wartens, P. Spencer, N. O'Neill, J. Long, J. Garlick
 *  For details, see http://github.com/chaos/lmt.
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

typedef struct lmt_db_struct *lmt_db_t;

int lmt_db_create (int readonly, const char *dbname, lmt_db_t *dbp);

int lmt_db_create_all (int readonly, List *dblp);

int lmt_db_list (char *user, char *pass, List *lp);

int lmt_db_drop (char *user, char *pass, char *fs);

int lmt_db_add (char *user, char *pass, char *fs, char *schema_vers,
                char *sql_schema);

void lmt_db_destroy (lmt_db_t db);

int lmt_db_insert_mds_data (lmt_db_t db, char *mdsname, char *mdtname,
                        float pct_cpu, uint64_t kbytes_free,
                        uint64_t kbytes_used, uint64_t inodes_free,
                        uint64_t inodes_used);
int lmt_db_insert_mds_ops_data (lmt_db_t db, char *mdtname, char *opname,
                        uint64_t samples, uint64_t sum, uint64_t sumsquares);
int lmt_db_insert_oss_data (lmt_db_t db, int quiet_noexist, char *name,
                        float pctcpu, float pctmem);
int lmt_db_insert_ost_data (lmt_db_t db, char *ossname, char *ostname,
                        uint64_t read_bytes, uint64_t write_bytes,
                        uint64_t kbytes_free, uint64_t kbytes_used,
                        uint64_t inodes_free, uint64_t inodes_used);
int lmt_db_insert_router_data (lmt_db_t db, char *name,
                        uint64_t bytes, float pct_cpu);

int lmt_db_insert_rpc_data (lmt_db_t db, char *ossname, char *ostname, char *histname, 
			    int bin, uint64_t read_count, uint64_t write_count);
/* accessors */

char *lmt_db_fsname (lmt_db_t db);

int lmt_db_lookup (lmt_db_t db, char *svctype, char *name);

typedef int (*lmt_db_map_f) (const char *key, void *arg);

int lmt_db_server_map (lmt_db_t db, char *svctype, lmt_db_map_f mf, void *arg);

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
