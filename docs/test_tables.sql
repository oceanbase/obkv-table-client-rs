CREATE TABLE `TEST_VARCHAR_TABLE_RANGE` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    `c3` bigint DEFAULT 0,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by range columns (c1) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);

CREATE TABLE `TEST_VARCHAR_TABLE_RANGE_CONCURRENT` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by range columns (c1) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);

CREATE TABLE `TEST_BLOB_TABLE_RANGE` (
    `c1` varchar(20) NOT NULL,
    `c2` blob DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by range columns (c1) (PARTITION p0 VALUES LESS THAN ('a'),PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);

CREATE TABLE `TEST_QUERY_TABLE_RANGE` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by range columns (c1) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);

CREATE TABLE `TEST_STREAM_QUERY_TABLE_RANGE` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by range columns (c1) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);

CREATE TABLE `TEST_TABLE_BATCH_RANGE` (
    `c1` bigint NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
PRIMARY KEY (`c1`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by range(`c1`) (partition p0 values less than(200), partition p1 values less than(500), partition p2 values less than(900));

CREATE TABLE `TEST_TABLE_BATCH_RANGE_COMPLEX` (
    `c1` bigint NOT NULL,
    `c1sk` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
PRIMARY KEY (`c1`, `c1sk`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by range(`c1`) (partition p0 values less than(200), partition p1 values less than(500), partition p2 values less than(900));

CREATE TABLE `TEST_VARCHAR_TABLE_KEY` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    `c3` bigint DEFAULT 0,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(c1) partitions 16;

CREATE TABLE `TEST_VARCHAR_TABLE_KEY_CONCURRENT` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(c1) partitions 16;

CREATE TABLE `TEST_BLOB_TABLE_KEY` (
    `c1` varchar(20) NOT NULL,
    `c2` blob DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(c1) partitions 16;

CREATE TABLE `TEST_QUERY_TABLE_KEY` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(c1) partitions 16;

CREATE TABLE `TEST_STREAM_QUERY_TABLE_KEY` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(c1) partitions 16;

CREATE TABLE `TEST_TABLE_BATCH_KEY` (
    `c1` varchar(20) NOT NULL,
    `c1sk` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`, `c1sk`)
) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(`c1`) partitions 16;

CREATE TABLE `TEST_TABLE_PARTITION_VARCHAR_KEY` (
    `c1` varchar(1024) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(`c1`) partitions 16;

CREATE TABLE `TEST_TABLE_PARTITION_COMPLEX_KEY` (
    `c1` bigint unsigned NOT NULL,
    `c2` varbinary(1024) NOT NULL,
    `c3` varchar(1024) NOT NULL,
    `c4` varchar(20) default NULL,
    PRIMARY KEY (`c1`, `c2`, `c3`)
) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(`c1`, `c2`, `c3`) partitions 16 ;

CREATE TABLE `TEST_TABLE_SUB_PARTITION_COMPLEX_KEY` (
    `c1` bigint NOT NULL,
    `c2` varbinary(1024) NOT NULL,
    `c3` varchar(1024) NOT NULL,
    `c4` varchar(1024) NOT NULL,
    `c5` varchar(20) default NULL,
    PRIMARY KEY (`c1`, `c2`, `c3`, `c4`)
) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(`c1`, `c2`, `c3`) subpartition by key(`c4`) subpartitions 4 partitions 16;

CREATE TABLE `TEST_VARCHAR_TABLE_HASH` (
    `c1` bigint(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    `c3` bigint DEFAULT 0,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by hash(c1) partitions 16;

CREATE TABLE `TEST_VARCHAR_TABLE_HASH_CONCURRENT` (
    `c1` bigint(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by hash(c1) partitions 16;

CREATE TABLE `TEST_BLOB_TABLE_HASH` (
    `c1` bigint(20) NOT NULL,
    `c2` blob DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by hash(c1) partitions 16;

CREATE TABLE `TEST_QUERY_TABLE_HASH` (
    `c1` bigint(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by hash(c1) partitions 16;

CREATE TABLE `TEST_STREAM_QUERY_TABLE_HASH` (
    `c1` bigint(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by hash(c1) partitions 16;

CREATE TABLE `TEST_TABLE_BATCH_HASH` (
`c1` bigint NOT NULL,
`c1sk` varchar(20) NOT NULL,
`c2` varchar(20) DEFAULT NULL,
PRIMARY KEY (`c1`, `c1sk`)) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by hash(`c1`) partitions 16;

CREATE TABLE `test_varchar_table` (
`c1` varchar(20) NOT NULL,
`c2` varchar(20) DEFAULT NULL,
`c3` bigint DEFAULT 0,
PRIMARY KEY (`c1`)
) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10;

CREATE TABLE TEST_HBASE_HASH(
    K bigint,
    Q varbinary(256),
    T bigint,
    V varbinary(1024),
PRIMARY KEY (K, Q, T)
) partition by hash(K) partitions 16;

CREATE TABLE `TEST_HBASE_PARTITION` (
    `K` varbinary(1024) NOT NULL,
    `Q` varbinary(256) NOT NULL,
    `T` bigint(20) NOT NULL,
    `V` varbinary(1024) DEFAULT NULL,
PRIMARY KEY (`K`, `Q`, `T`)) DEFAULT CHARSET = utf8mb4 COLLATE UTF8MB4_BIN COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3  BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
partition by key(k) partitions 15;

create table TEST_HBASE_RANGE(
    K varbinary(1024) NOT NULL,
    Q varbinary(256),
    T bigint,
    V varbinary(1024),
primary key(K, Q, T)) DEFAULT CHARSET = utf8mb4 COLLATE UTF8MB4_BIN COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3  BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
PARTITION BY RANGE columns (K) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);

create table cse_data_20190308_1 (
    series_id bigint NOT NULL,
    field_id int NOT NULL,
    start_time INT NOT NULL,
    value MEDIUMBLOB NOT NULL,
    extra_value MEDIUMBLOB default NULL,
PRIMARY KEY(series_id, field_id, start_time))
partition by range(start_time) (partition p00 values less than(3600), partition p01 values less than(7200), partition p02 values less than(10800),
partition p03 values less than(14400), partition p04 values less than(18000), partition p05 values less than(21600), partition p06 values less than(25200),
partition p07 values less than(28800), partition p08 values less than(32400), partition p09 values less than(36000), partition p10 values less than(39600),
partition p11 values less than(43200), partition p12 values less than(46800), partition p13 values less than(50400), partition p14 values less than(54000),
partition p15 values less than(57600), partition p16 values less than(61200), partition p17 values less than(64800), partition p18 values less than(68400),
partition p19 values less than(72000), partition p20 values less than(75600), partition p21 values less than(79200), partition p22 values less than(82800),
partition p23 values less than(86400), partition p24 values less than(MAXVALUE));

create table cse_meta_data_0 (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(1024) NOT NULL DEFAULT '',
    data_table_name VARCHAR(100) NOT NULL,
    data_table_start_time_ms INT NOT NULL,
    status TINYINT NOT NULL DEFAULT 0,
    start_time_ms BIGINT(20) DEFAULT 0,
    end_time_ms BIGINT(20) DEFAULT 0,
    interval_ms INT DEFAULT 0,
PRIMARY KEY(id), UNIQUE KEY data_table_loc(data_table_name, data_table_start_time_ms));

create table cse_index_1 (
    measurement VARBINARY(1024) NOT NULL,
    tag_key VARBINARY(1024) NOT NULL,
    tag_value VARBINARY(1024) NOT NULL,
    series_ids MEDIUMBLOB NOT NULL,
    PRIMARY KEY(measurement, tag_key, tag_value))
partition by key(measurement, tag_key, tag_value) partitions 13;

create table cse_field_1 (
    measurement VARBINARY(1024) NOT NULL,
    field_name VARBINARY(1024) NOT NULL,
    field_type INT NOT NULL,
    id INT NOT NULL,
    PRIMARY KEY(measurement, field_name))
partition by key(measurement, field_name) partitions 13;

create table cse_series_key_to_id_1 (
    series_key VARBINARY(8096) NOT NULL,
    series_id BIGINT NOT NULL,
PRIMARY KEY(series_key), KEY index_id(series_id));

CREATE TABLE test_aggregation (
    `c1` int NOT NULL,
    `c2` tinyint unsigned NOT NULL,
    `c3` double DEFAULT NULL,
    PRIMARY KEY(`c1`, `c2`)
) PARTITION BY KEY(`c1`) PARTITIONS 200;

CREATE TABLE test_partition_aggregation (
    `c1` int NOT NULL,
    `c2` bigint NOT NULL,
    `c3` double DEFAULT NULL,
    `c4` varchar(5) DEFAULT NULL,
    PRIMARY KEY(`c1`, `c2`)
) PARTITION BY KEY(`c1`) PARTITIONS 200;
