create table usertable (
    ycsb_key varchar(255) primary key,
    field0 varchar(255),
    field1 varchar(255),
    field2 varchar(255),
    field3 varchar(255),
    field4 varchar(255),
    field5 varchar(255),
    field6 varchar(255),
    field7 varchar(255),
    field8 varchar(255),
    field9 varchar(255))
partition by key(ycsb_key) partitions 3;
