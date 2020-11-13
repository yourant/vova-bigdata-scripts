CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_goods_project_inc (
    -- maxwell event data
    `event_id` STRING,
    `event_table` STRING,
    `event_type` STRING,
    `event_commit` BOOLEAN,
    `event_date` BIGINT,
    -- now data 
    `id` bigint comment 'id',
    `goods_id` bigint,
    `party_id` bigint comment '组织',
    `created_at` bigint comment '生成时间戳bigint',
    `updated_at` bigint comment '更新时间戳bigint',
    `deleted_at` string,
    `on_sale_time` string comment '上架时间',
    `on_sale_staff` string comment '上架人',
    `off_sale_time` string comment '下架时间',
    `shop_price` decimal(10,2) comment '商品价格，按项目分',
    `market_price` decimal(10,2),
    `is_tort` string comment '是否侵权 N :未侵权 Y：已侵权',
    `risk_level` string comment '风控等级：h_danger：一级,m_danger：二级,l_danger：三级,danger：四级,l_secure：五级,secure：六级',
    `is_on_sale` tinyint,
    `is_delete` tinyint,
    `is_display` tinyint,
    `virtual_goods_id` string comment '虚拟id',
    `goods_selector` string comment '选款人'
) COMMENT '商品组织信息表'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_goods_project_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id,
    o_raw.`table` AS event_table,
    o_raw.type AS event_type,
    cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
    cast(o_raw.ts AS BIGINT) AS event_date,
    o_raw.id,
    o_raw.goods_id,
    o_raw.party_id,
    o_raw.if(o_raw.created_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.created_at, "yyyy-MM-dd HH:mm:ss"), 0) AS created_at,
    o_raw.if(o_raw.updated_at != '0000-00-00 00:00:00', unix_timestamp(o_raw.updated_at, "yyyy-MM-dd HH:mm:ss"), 0) AS updated_at,
    o_raw.deleted_at,
    o_raw.on_sale_time,
    o_raw.on_sale_staff,
    o_raw.off_sale_time,
    o_raw.shop_price,
    o_raw.market_price,
    o_raw.is_tort,
    o_raw.risk_level,
    o_raw.is_on_sale,
    o_raw.is_delete,
    o_raw.is_display,
    o_raw.virtual_goods_id,
    o_raw.goods_selector,
    hour as hour
from tmp.tmp_fd_dmc_goods_project
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'id', 'goods_id', 'party_id', 'created_at', 'updated_at', 'deleted_at', 'on_sale_time', 'on_sale_staff', 'off_sale_time', 'shop_price', 'market_price', 'is_tort', 'risk_level', 'is_on_sale', 'is_delete', 'is_display', 'virtual_goods_id', 'goods_selector') o_raw
AS `table`, ts, `commit`, xid, type, old, id, goods_id, party_id, created_at, updated_at, deleted_at, on_sale_time, on_sale_staff, off_sale_time, shop_price, market_price, is_tort, risk_level, is_on_sale, is_delete, is_display, virtual_goods_id, goods_selector
where dt = '${hiveconf:dt}';
