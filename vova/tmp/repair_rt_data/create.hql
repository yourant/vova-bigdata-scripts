drop table tmp.tmp_gmv_rt;
CREATE  TABLE IF NOT EXISTS tmp.tmp_gmv_rt
(
    datasource       string comment '数据平台',
    country          string comment '数据平台',
    cur_date         string comment '数据平台',
    cur_hour         string comment '数据平台',
    gmv              decimal(10,2) comment '数据平台',
    payed_num        bigint comment '数据平台',
    payed_uv         bigint comment '数据平台'
) COMMENT '实时表' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table tmp.tmp_uv_rt;
CREATE external TABLE IF NOT EXISTS tmp.tmp_uv_rt
(
    cur_date         string comment '数据平台',
    cur_hour         string comment '数据平台',
    datasource       string comment '数据平台',
    country          string comment '数据平台',
    uv         bigint comment '数据平台'
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



