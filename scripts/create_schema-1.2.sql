#
# LMT3 SCHEMA 1.2 - begin
#
create table if not exists FILESYSTEM_INFO (
    FILESYSTEM_ID   integer         not null auto_increment,
    FILESYSTEM_NAME varchar(128)    not null,
    FILESYSTEM_MOUNT_NAME varchar(64) not null,
    SCHEMA_VERSION float not null,
    primary key (FILESYSTEM_ID),
    index(FILESYSTEM_ID)
);
create table if not exists OSS_INFO (
    OSS_ID          integer         not null auto_increment,
    FILESYSTEM_ID   integer         not null,
    HOSTNAME        varchar(128)    not null,
    FAILOVERHOST    varchar(128),
    foreign key(FILESYSTEM_ID) references FILESYSTEM_INFO(FILESYSTEM_ID),
    primary key (OSS_ID),
    index(OSS_ID)
);
create table if not exists OSS_INTERFACE_INFO (
    OSS_INTERFACE_ID   integer      not null auto_increment,
    OSS_ID             integer      not null,
    OSS_INTERFACE_NAME varchar(128) not null,
    EXPECTED_RATE      integer,
    primary key (OSS_INTERFACE_ID),
    index(OSS_INTERFACE_ID)
);
create table if not exists OST_INFO (
    OST_ID          integer         not null auto_increment,
    OSS_ID          integer         not null,
    OST_NAME        varchar(128)    not null,
    HOSTNAME        varchar(128)    not null,
    OFFLINE         boolean,
    DEVICE_NAME     varchar(128),
    foreign key(OSS_ID) references OSS_INFO(OSS_ID),
    primary key (OST_ID),
    index(OST_ID)
);
create table if not exists MDS_INFO (
    MDS_ID          integer         not null auto_increment,
    FILESYSTEM_ID   integer         not null,
    MDS_NAME        varchar(128)    not null,
    HOSTNAME        varchar(128)    not null,
    DEVICE_NAME     varchar(128),
    foreign key(FILESYSTEM_ID) references FILESYSTEM_INFO(FILESYSTEM_ID),
    primary key (MDS_ID),
    index(MDS_ID)
);
create table if not exists ROUTER_INFO (
    ROUTER_ID       integer         not null auto_increment,
    ROUTER_NAME     varchar(128)    not null,
    HOSTNAME        varchar(128)    not null,
    ROUTER_GROUP_ID integer         not null,
    primary key(ROUTER_ID),
    index(ROUTER_ID)
);
create table if not exists OPERATION_INFO (
    OPERATION_ID    integer         not null auto_increment,
    OPERATION_NAME  varchar(64)     not null unique,
    UNITS           varchar(16)     not null,
    primary key(OPERATION_ID),
    index(OPERATION_ID),
    index(OPERATION_NAME)
);create table if not exists TIMESTAMP_INFO (
    TS_ID           int unsigned    not null auto_increment,
    TIMESTAMP       datetime        not null,
    primary key(TS_ID),
    key(TIMESTAMP),
    index(TS_ID),
    index(TIMESTAMP)
);
create table if not exists VERSION (
    VERSION_ID      integer         not null auto_increment,
    VERSION         varchar(255)    not null,
    TS_ID           int unsigned    not null,
    primary key(VERSION_ID),
    key(TS_ID),
    foreign key(TS_ID) references TIMESTAMP_ID(TS_ID),
    index(VERSION_ID),
    index(TS_ID)
);
create table if not exists EVENT_INFO (
    EVENT_ID        integer         not null auto_increment,
    EVENT_NAME      varchar(64)     not null,
    primary key(EVENT_ID),
    index(EVENT_ID)
);
create table if not exists OST_VARIABLE_INFO (
    VARIABLE_ID     integer         not null auto_increment,
    VARIABLE_NAME   varchar(64)     not null,
    VARIABLE_LABEL  varchar(64),
    THRESH_TYPE     integer,
    THRESH_VAL1     float,
    THRESH_VAL2     float,
    primary key (VARIABLE_ID),
    key (VARIABLE_NAME),
    index(VARIABLE_ID)
);
create table if not exists OSS_VARIABLE_INFO (
    VARIABLE_ID     integer         not null auto_increment,
    VARIABLE_NAME   varchar(64)     not null,
    VARIABLE_LABEL  varchar(64),
    THRESH_TYPE     integer,
    THRESH_VAL1     float,
    THRESH_VAL2     float,
    primary key (VARIABLE_ID),
    key (VARIABLE_NAME),
    index(VARIABLE_ID)
);
create table if not exists MDS_VARIABLE_INFO (
    VARIABLE_ID     integer         not null auto_increment,
    VARIABLE_NAME   varchar(64)     not null,
    VARIABLE_LABEL  varchar(64),
    THRESH_TYPE     integer,
    THRESH_VAL1     float,
    THRESH_VAL2     float,
    primary key (VARIABLE_ID),
    key (VARIABLE_NAME),
    index(VARIABLE_ID)
);
create table if not exists ROUTER_VARIABLE_INFO (
    VARIABLE_ID     integer         not null auto_increment,
    VARIABLE_NAME   varchar(64)     not null,
    VARIABLE_LABEL  varchar(64),
    THRESH_TYPE     integer,
    THRESH_VAL1     float,
    THRESH_VAL2     float,
    primary key (VARIABLE_ID),
    key (VARIABLE_NAME),
    index(VARIABLE_ID)
);
create table if not exists OST_DATA (
    OST_ID          integer         not null comment 'OST ID',
    TS_ID           int unsigned    not null comment 'TS ID',
    READ_BYTES      bigint                   comment 'READ BYTES',
    WRITE_BYTES     bigint                   comment 'WRITE BYTES',
    PCT_CPU         float                    comment '%CPU',
    KBYTES_FREE     bigint                   comment 'KBYTES FREE',
    KBYTES_USED     bigint                   comment 'KBYTES USED',
    INODES_FREE     bigint                   comment 'INODES FREE',
    INODES_USED     bigint                   comment 'INODES USED',
    primary key (OST_ID,TS_ID),
    foreign key(OST_ID) references OST_INFO(OST_ID),
    foreign key(TS_ID)  references TIMESTAMP_INFO(TS_ID),
    index(TS_ID),
    index(OST_ID)
    ) MAX_ROWS=2000000000;
create table if not exists OST_OPS_DATA (
    OST_ID          integer         not null comment 'OST ID',
    TS_ID           int unsigned    not null comment 'TS ID',
    OPERATION_ID    integer         not null comment 'OP ID',
    SAMPLES         bigint                   comment 'SAMPLES',
    primary key (OST_ID,TS_ID,OPERATION_ID),
    foreign key(OST_ID) references OST_INFO(OST_ID),
    foreign key(TS_ID)  references TIMESTAMP_INFO(TS_ID),
    foreign key(OPERATION_ID)  references OPERATION_INFO(OPERATION_ID),
    index(TS_ID),
    index(OST_ID),
    index(OPERATION_ID)
    ) MAX_ROWS=2000000000;
create table if not exists OSS_DATA (
    OSS_ID          integer         not null comment 'OSS ID',
    TS_ID           int unsigned    not null comment 'TS ID',
    PCT_CPU         float                    comment '%CPU',
    PCT_MEMORY      float                    comment '%MEM',
    primary key (OSS_ID,TS_ID),
    foreign key(OSS_ID) references OSS_INFO(OSS_ID),
    foreign key(TS_ID) references TIMESTAMP_ID(TS_ID),
    index(TS_ID),
    index(OSS_ID)
);
create table if not exists OSS_INTERFACE_DATA (
    OSS_INTERFACE_ID integer        not null comment 'OSS INTER ID',
    TS_ID           int unsigned    not null comment 'TS ID',
    READ_BYTES      bigint                   comment 'READ BYTES',
    WRITE_BYTES     bigint                   comment 'WRITE BYTES',
    ERROR_COUNT     integer                  comment 'ERR COUNT',
    LINK_STATUS     integer                  comment 'LINK STATUS',
    ACTUAL_RATE     integer                  comment 'ACT RATE',
    primary key (OSS_INTERFACE_ID,TS_ID),
    foreign key(OSS_INTERFACE_ID) references OSS_INTERFACE_INFO(OSS_INTERFACE_ID),
    foreign key(TS_ID) references TIMESTAMP_ID(TS_ID),
    index(TS_ID),
    index(OSS_INTERFACE_ID)
);
create table if not exists MDS_DATA (
    MDS_ID          integer         not null comment 'MDS ID',
    TS_ID           int unsigned    not null comment 'TS ID',
    PCT_CPU         float                    comment '%CPU',
    KBYTES_FREE     bigint                   comment 'KBYTES FREE',
    KBYTES_USED     bigint                   comment 'KBYTES USED',
    INODES_FREE     bigint                   comment 'INODES FREE',
    INODES_USED     bigint                   comment 'INODES USED',
    primary key (MDS_ID,TS_ID),
    foreign key(MDS_ID) references MDS_INFO(MDS_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    index(TS_ID),
    index(MDS_ID)
    ) MAX_ROWS=2000000000;
create table if not exists MDS_OPS_DATA (
    MDS_ID          integer         not null comment 'OST ID',
    TS_ID           int unsigned    not null comment 'TS ID',
    OPERATION_ID    integer         not null comment 'OP ID',
    SAMPLES         bigint                   comment 'SAMPLES',
    SUM             bigint                   comment 'SUM',
    SUMSQUARES      bigint                   comment 'SUM SQUARES',
    foreign key(MDS_ID) references MDS_INFO(MDS_ID),
    foreign key(TS_ID)  references TIMESTAMP_INFO(TS_ID),
    foreign key(OPERATION_ID)  references OPERATION_INFO(OPERATION_ID),
    index(TS_ID),
    index(MDS_ID),
    index(OPERATION_ID)
    ) MAX_ROWS=2000000000;
create table if not exists ROUTER_DATA (
    ROUTER_ID       integer         not null comment 'ROUTER ID',
    TS_ID           int unsigned    not null comment 'TS ID',
    BYTES           bigint                   comment 'BYTES',
    PCT_CPU         float                    comment '%CPU',
    primary key (ROUTER_ID,TS_ID),
    foreign key(ROUTER_ID) references ROUTER_INFO(ROUTER_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    index(ROUTER_ID),
    index(TS_ID)
    ) MAX_ROWS=2000000000;
create table if not exists EVENT_DATA (
    EVENT_ID        integer         not null,
    TS_ID           int unsigned    not null comment 'TS ID',
    OSS_ID          integer                  comment 'OSS ID',
    OST_ID          integer                  comment 'OST ID',
    MDS_ID          integer                  comment 'MDS ID',
    ROUTER_ID       integer                  comment 'ROUTER ID',
    COMMENT         varchar(4096)            comment 'COMMENT',
    foreign key(EVENT_ID) references EVENT_INFO(EVENT_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(OST_ID) references OST_INFO(OST_ID),
    foreign key(MDS_ID) references MDS_INFO(MDS_ID),
    foreign key(ROUTER_ID) references ROUTER_INFO(ROUTER_ID),
    index(EVENT_ID),
    index(TS_ID),
    index(OST_ID)
    ) MAX_ROWS=2000000000;
create table if not exists OST_AGGREGATE_HOUR (
    OST_ID          integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (OST_ID,TS_ID,VARIABLE_ID),
    foreign key(OST_ID) references OST_INFO(OST_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references OST_VARIABLE_INFO(VARIABLE_ID),
    index(OST_ID),
    index(TS_ID),
    index(VARIABLE_ID)
    ) MAX_ROWS=2000000000;
create table if not exists OST_AGGREGATE_DAY (
    OST_ID          integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (OST_ID,TS_ID,VARIABLE_ID),
    foreign key(OST_ID) references OST_INFO(OST_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references OST_VARIABLE_INFO(VARIABLE_ID),
    index(OST_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists OST_AGGREGATE_WEEK (
    OST_ID          integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (OST_ID,TS_ID,VARIABLE_ID),
    foreign key(OST_ID) references OST_INFO(OST_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references OST_VARIABLE_INFO(VARIABLE_ID),
    index(OST_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists OST_AGGREGATE_MONTH (
    OST_ID          integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (OST_ID,TS_ID,VARIABLE_ID),
    foreign key(OST_ID) references OST_INFO(OST_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references OST_VARIABLE_INFO(VARIABLE_ID),
    index(OST_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists OST_AGGREGATE_YEAR (
    OST_ID          integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (OST_ID,TS_ID,VARIABLE_ID),
    foreign key(OST_ID) references OST_INFO(OST_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references OST_VARIABLE_INFO(VARIABLE_ID),
    index(OST_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists ROUTER_AGGREGATE_HOUR (
    ROUTER_ID       integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (ROUTER_ID,TS_ID,VARIABLE_ID),
    foreign key(ROUTER_ID) references ROUTER_INFO(ROUTER_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references ROUTER_VARIABLE_INFO(VARIABLE_ID),
    index(ROUTER_ID),
    index(TS_ID),
    index(VARIABLE_ID)
    ) MAX_ROWS=2000000000;
create table if not exists ROUTER_AGGREGATE_DAY (
    ROUTER_ID       integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (ROUTER_ID,TS_ID,VARIABLE_ID),
    foreign key(ROUTER_ID) references ROUTER_INFO(ROUTER_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references ROUTER_VARIABLE_INFO(VARIABLE_ID),
    index(ROUTER_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists ROUTER_AGGREGATE_WEEK (
    ROUTER_ID       integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (ROUTER_ID,TS_ID,VARIABLE_ID),
    foreign key(ROUTER_ID) references ROUTER_INFO(ROUTER_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references ROUTER_VARIABLE_INFO(VARIABLE_ID),
    index(ROUTER_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists ROUTER_AGGREGATE_MONTH (
    ROUTER_ID       integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (ROUTER_ID,TS_ID,VARIABLE_ID),
    foreign key(ROUTER_ID) references ROUTER_INFO(ROUTER_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references ROUTER_VARIABLE_INFO(VARIABLE_ID),
    index(ROUTER_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists ROUTER_AGGREGATE_YEAR (
    ROUTER_ID       integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (ROUTER_ID,TS_ID,VARIABLE_ID),
    foreign key(ROUTER_ID) references ROUTER_INFO(ROUTER_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references ROUTER_VARIABLE_INFO(VARIABLE_ID),
    index(ROUTER_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists MDS_AGGREGATE_HOUR (
    MDS_ID          integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (MDS_ID,TS_ID,VARIABLE_ID),
    foreign key(MDS_ID) references MDS_INFO(MDS_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references MDS_VARIABLE_INFO(VARIABLE_ID),
    index(MDS_ID),
    index(TS_ID),
    index(VARIABLE_ID)
    ) MAX_ROWS=2000000000;
create table if not exists MDS_AGGREGATE_DAY (
    MDS_ID          integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (MDS_ID,TS_ID,VARIABLE_ID),
    foreign key(MDS_ID) references MDS_INFO(MDS_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references MDS_VARIABLE_INFO(VARIABLE_ID),
    index(MDS_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists MDS_AGGREGATE_WEEK (
    MDS_ID          integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (MDS_ID,TS_ID,VARIABLE_ID),
    foreign key(MDS_ID) references MDS_INFO(MDS_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references MDS_VARIABLE_INFO(VARIABLE_ID),
    index(MDS_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists MDS_AGGREGATE_MONTH (
    MDS_ID          integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (MDS_ID,TS_ID,VARIABLE_ID),
    foreign key(MDS_ID) references MDS_INFO(MDS_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references MDS_VARIABLE_INFO(VARIABLE_ID),
    index(MDS_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists MDS_AGGREGATE_YEAR (
    MDS_ID          integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    AGGREGATE       float,
    MINVAL          float,
    MAXVAL          float,
    AVERAGE         float,
    NUM_SAMPLES     integer,
    primary key (MDS_ID,TS_ID,VARIABLE_ID),
    foreign key(MDS_ID) references MDS_INFO(MDS_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references MDS_VARIABLE_INFO(VARIABLE_ID),
    index(MDS_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists FILESYSTEM_AGGREGATE_HOUR (
    FILESYSTEM_ID   integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    OST_AGGREGATE   float,
    OST_MINVAL      float,
    OST_MAXVAL      float,
    OST_AVERAGE     float,
    primary key (FILESYSTEM_ID,TS_ID,VARIABLE_ID),
    foreign key(FILESYSTEM_ID) references FILESYSTEM_INFO(FILESYSTEM_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references OST_VARIABLE_INFO(VARIABLE_ID),
    index(FILESYSTEM_ID),
    index(TS_ID),
    index(VARIABLE_ID)
    ) MAX_ROWS=2000000000;
create table if not exists FILESYSTEM_AGGREGATE_DAY (
    FILESYSTEM_ID   integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    OST_AGGREGATE   float,
    OST_MINVAL      float,
    OST_MAXVAL      float,
    OST_AVERAGE     float,
    primary key (FILESYSTEM_ID,TS_ID,VARIABLE_ID),
    foreign key(FILESYSTEM_ID) references FILESYSTEM_INFO(FILESYSTEM_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references OST_VARIABLE_INFO(VARIABLE_ID),
    index(FILESYSTEM_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists FILESYSTEM_AGGREGATE_WEEK (
    FILESYSTEM_ID   integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    OST_AGGREGATE   float,
    OST_MINVAL      float,
    OST_MAXVAL      float,
    OST_AVERAGE     float,
    primary key (FILESYSTEM_ID,TS_ID,VARIABLE_ID),
    foreign key(FILESYSTEM_ID) references FILESYSTEM_INFO(FILESYSTEM_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references OST_VARIABLE_INFO(VARIABLE_ID),
    index(FILESYSTEM_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists FILESYSTEM_AGGREGATE_MONTH (
    FILESYSTEM_ID   integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    OST_AGGREGATE   float,
    OST_MINVAL      float,
    OST_MAXVAL      float,
    OST_AVERAGE     float,
    primary key (FILESYSTEM_ID,TS_ID,VARIABLE_ID),
    foreign key(FILESYSTEM_ID) references FILESYSTEM_INFO(FILESYSTEM_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references OST_VARIABLE_INFO(VARIABLE_ID),
    index(FILESYSTEM_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
create table if not exists FILESYSTEM_AGGREGATE_YEAR (
    FILESYSTEM_ID   integer         not null,
    TS_ID           int unsigned    not null,
    VARIABLE_ID     integer         not null,
    OST_AGGREGATE   float,
    OST_MINVAL      float,
    OST_MAXVAL      float,
    OST_AVERAGE     float,
    primary key (FILESYSTEM_ID,TS_ID,VARIABLE_ID),
    foreign key(FILESYSTEM_ID) references FILESYSTEM_INFO(FILESYSTEM_ID),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(VARIABLE_ID) references OST_VARIABLE_INFO(VARIABLE_ID),
    index(FILESYSTEM_ID),
    index(TS_ID),
    index(VARIABLE_ID)
);
# New tables for RPC stats info (/proc/fs/lustre/obdfilter/<ost>/brw_stats
create table if not exists BRW_STATS_INFO (
    STATS_ID        int unsigned not null auto_increment,
    CODE            varchar(16),
    DESCRIPTION	    varchar(128),
    UNITS           varchar(16),
    primary key (STATS_ID),
    index(STATS_ID)
);
create table if not exists BRW_STATS_DATA (
    TS_ID           int unsigned    not null,
    STATS_ID        int unsigned    not null,
    BIN             integer         not null,
    READ_COUNT      integer,
    WRITE_COUNT     integer,
    primary key (TS_ID,STATS_ID,BIN),
    foreign key(TS_ID) references TIMESTAMP_INFO(TS_ID),
    foreign key(STATS_ID) references BRW_STATS_INFO(STATS_ID),
    index(TS_ID),
    index(STATS_ID),
    index(BIN)
);

insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('open', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('close', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('mknod', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('link', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('unlink', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('mkdir', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('rmdir', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('rename', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('getxattr', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('setxattr', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('iocontrol', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('get_info', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('set_info_async', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('attach', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('detach', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('setup', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('precleanup', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('cleanup', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('process_config', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('postrecov', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('add_conn', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('del_conn', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('connect', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('reconnect', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('disconnect', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('statfs', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('statfs_async', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('packmd', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('unpackmd', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('checkmd', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('preallocate', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('precreate', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('create', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('destroy', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('setattr', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('setattr_async', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('getattr', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('getattr_async', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('brw', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('brw_async', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('prep_async_page', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('reget_short_lock', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('release_short_lock', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('queue_async_io', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('queue_group_io', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('trigger_group_io', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('set_async_flags', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('teardown_async_page', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('merge_lvb', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('adjust_kms', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('punch', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('sync', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('migrate', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('copy', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('iterate', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('preprw', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('commitrw', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('enqueue', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('match', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('change_cbdata', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('cancel', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('cancel_unused', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('join_lru', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('init_export', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('destroy_export', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('extent_calc', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('llog_init', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('llog_finish', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('pin', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('unpin', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('import_event', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('notify', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('health_check', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('quotacheck', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('quotactl', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('quota_adjust_quint', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('ping', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('register_page_removal_cb', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('unregister_page_removal_cb', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('register_lock_cancel_cb', 'reqs');
insert ignore into OPERATION_INFO (OPERATION_NAME, UNITS) values ('unregister_lock_cancel_cb', 'reqs');
insert ignore into OSS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('PCT_MEM','%Mem', 0);
insert ignore into OSS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('READ_RATE','Read Rate', 0);
insert ignore into OSS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('WRITE_RATE','Write Rate', 0);
insert ignore into OSS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('ACTUAL_RATE','Actual Rate', 0);
insert ignore into OSS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE, THRESH_VAL1) values ('LINK_STATUS','Link Status',      1,  1.);
insert ignore into OSS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE, THRESH_VAL1, THRESH_VAL2) values ('PCT_CPU',     '%CPU',           3, 90., 101.);
insert ignore into OSS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE, THRESH_VAL1, THRESH_VAL2) values ('ERROR_COUNT', 'Error Count',    3,  1., 100.);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('READ_BYTES','Bytes Read', 0);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('WRITE_BYTES','Bytes Written', 0);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('READ_RATE', 'Read Rate', 0);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('WRITE_RATE', 'Write Rate', 0);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('KBYTES_FREE', 'KB Free', 0);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('KBYTES_USED', 'KB Used', 0);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('INODES_FREE', 'Inodes Free', 0);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('INODES_USED', 'Inodes Used', 0);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE, THRESH_VAL1, THRESH_VAL2) values ('PCT_CPU',    '%CPU',    3, 90., 101.);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE, THRESH_VAL1, THRESH_VAL2) values ('PCT_KBYTES', '%KB',     3, 95., 100.);
insert ignore into OST_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE, THRESH_VAL1, THRESH_VAL2) values ('PCT_INODES', '%Inodes', 3, 95., 100.);
insert ignore into MDS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('KBYTES_FREE','KB Free', 0);
insert ignore into MDS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('KBYTES_USED','KB Used', 0);
insert ignore into MDS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('INODES_FREE','Inodes Free', 0);
insert ignore into MDS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('INODES_USED','Inodes Used', 0);
insert ignore into MDS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE, THRESH_VAL1, THRESH_VAL2) values ('PCT_CPU',    '%CPU',    3, 90., 101.);
insert ignore into MDS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE, THRESH_VAL1, THRESH_VAL2) values ('PCT_KBYTES', '%KB',     3, 95., 100.);
insert ignore into MDS_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE, THRESH_VAL1, THRESH_VAL2) values ('PCT_INODES', '%Inodes', 3, 95., 100.);
insert ignore into ROUTER_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('BYTES','Bytes', 0);
insert ignore into ROUTER_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('RATE', 'Rate', 0);
insert ignore into ROUTER_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE) values ('BANDWIDTH','Bandwidth', 0);
insert ignore into ROUTER_VARIABLE_INFO (VARIABLE_NAME,VARIABLE_LABEL,THRESH_TYPE, THRESH_VAL1, THRESH_VAL2) values ('PCT_CPU', '%CPU', 3, 90., 101.);

# Data for new table for RPC stats info (/proc/fs/lustre/obdfilter/<ost>/brw_stats
insert ignore into BRW_STATS_INFO (CODE, DESCRIPTION, UNITS) values ('BRW_RPC', 'pages per bulk r/w', 'pages');
insert ignore into BRW_STATS_INFO (CODE, DESCRIPTION, UNITS) values ('BRW_DISPAGES', 'discontiguous pages', 'pages');
insert ignore into BRW_STATS_INFO (CODE, DESCRIPTION, UNITS) values ('BRW_DISBLOCKS', 'discontiguous blocks', 'blocks');
insert ignore into BRW_STATS_INFO (CODE, DESCRIPTION, UNITS) values ('BRW_FRAG', 'disk fragmented I/Os', 'I/Os');
insert ignore into BRW_STATS_INFO (CODE, DESCRIPTION, UNITS) values ('BRW_FLIGHT', 'disk I/Os in flight', 'I/Os');
insert ignore into BRW_STATS_INFO (CODE, DESCRIPTION, UNITS) values ('BRW_IOTIME', 'I/O time (1/1000s)', 'ms');
insert ignore into BRW_STATS_INFO (CODE, DESCRIPTION, UNITS) values ('BRW_IOSIZE', 'disk I/O size', 'bytes');
#
# LMT3 SCHEMA 1.2 - end
#
