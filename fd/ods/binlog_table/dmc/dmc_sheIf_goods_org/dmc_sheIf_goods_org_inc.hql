CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_sheIf_goods_org_inc (
    -- maxwell event data
    `event_id` STRING,
    `event_table` STRING,
    `event_type` STRING,
    `event_commit` BOOLEAN,
    `event_date` BIGINT,
    -- now data 
    `id` bigint comment '',
    `org_name` string comment '组织名称',
    `virtual_id` bigint comment '虚拟id',
    `sheIf_goods_id` bigint comment '上架商品表主键id',
    `updated_at` bigint comment '',
    `created_at` bigint comment '',
    `deleted_at` bigint comment '',
    `party_id` string comment '',
    `is_sheIf` bigint comment '是否在该组织上过架 0：未上架 1：已上架 2：等待上架',
    `operate_price_user_email` string comment '运营定价人邮箱',
    `operate_price_user` string comment '运营定价人',
    `sheIf_time` bigint comment '上架时间',
    `sheIf_user_email` string comment '上架用户邮箱',
    `sheIf_user` string comment '上架用户名称',
    `sheIf_note` string comment '上架备注',
    `extend_goods_id` bigint comment 'editor生成的goodsId',
    `pg_id` bigint comment 'fam_provider_goods表主键id',
    `status` string comment 'fam_provider_goods_feedback表pgf_status状态',
    `test_location` string comment '测试坑位',
    `test_note` string comment '测试备注'
) COMMENT '上架商品对应组织'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_sheIf_goods_org_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.id,
    o_raw.org_name,
    o_raw.virtual_id,
    o_raw.sheIf_goods_id,
    if(o_raw.updated_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.updated_at, "yyyy-MM-dd HH:mm:ss"), 0) AS updated_at,
    if(o_raw.created_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.created_at, "yyyy-MM-dd HH:mm:ss"), 0) AS created_at,
    if(o_raw.deleted_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.deleted_at, "yyyy-MM-dd HH:mm:ss"), 0) AS deleted_at,
    o_raw.party_id,
    o_raw.is_sheIf,
    o_raw.operate_price_user_email,
    o_raw.operate_price_user,
    if(o_raw.sheIf_time != '0000-00-00 00:00:00', unix_timestamp(o_raw.sheIf_time, "yyyy-MM-dd HH:mm:ss"), 0) AS sheIf_time,
    o_raw.sheIf_user_email,
    o_raw.sheIf_user,
    o_raw.sheIf_note,
    o_raw.extend_goods_id,
    o_raw.pg_id,
    o_raw.status,
    o_raw.test_location,
    o_raw.test_note,
    hour as hour
from tmp.tmp_fd_dmc_sheIf_goods_org
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'org_name', 'virtual_id', 'sheIf_goods_id', 'updated_at', 'created_at', 'deleted_at', 'party_id', 'is_sheIf', 'operate_price_user_email', 'operate_price_user', 'sheIf_time', 'sheIf_user_email', 'sheIf_user', 'sheIf_note', 'extend_goods_id', 'pg_id', 'status', 'test_location', 'test_note') o_raw
AS `table`, ts, `commit`, xid, type, old, id, org_name, virtual_id, sheIf_goods_id, updated_at, created_at, deleted_at, party_id, is_sheIf, operate_price_user_email, operate_price_user, sheIf_time, sheIf_user_email, sheIf_user, sheIf_note, extend_goods_id, pg_id, status, test_location, test_note
where dt = '${hiveconf:dt}';
