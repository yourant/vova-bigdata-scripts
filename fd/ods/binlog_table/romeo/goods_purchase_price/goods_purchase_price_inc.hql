CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_goods_purchase_price_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    goods_id                bigint comment '网站商品ID，非erp商品ID',
    provider_id             bigint comment '供应商的编号ID',
    dispatch_sn             string comment '商品工单号',
    price                   decimal(10, 2) comment '给供应商的价格',
    wrap_price              decimal(10, 2) comment '给供应商披肩的价格',
    is_delete               tinyint comment '是否删除：1删除 0正常',
    ctime                   bigint comment '创建时间',
    pk_cat_id               tinyint,
    last_purchase_price     string,
    last_purchase_wrapprice string,
    last_purchase_provider  string,
    provider_id2            bigint comment '供应商的编号ID',
    provider_id3            bigint comment '供应商的编号ID',
    provider_id4            bigint comment '供应商的编号ID',
    provider_id5            bigint comment '供应商的编号ID',
    ratio                   decimal(10, 4) comment 'provider_id 的分单比例',
    ratio2                  decimal(10, 4) comment 'provider_id2 的分单比例',
    ratio3                  decimal(10, 4) comment 'provider_id3 的分单比例',
    ratio4                  decimal(10, 4) comment 'provider_id4 的分单比例',
    ratio5                  decimal(10, 4) comment 'provider_id5 的分单比例',
    color                   string comment 'provider_id 的颜色分配',
    color2                  string comment 'provider_id2 的颜色分配',
    color3                  string comment 'provider_id3 的颜色分配',
    color4                  string comment 'provider_id4 的颜色分配',
    color5                  string comment 'provider_id5 的颜色分配'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_goods_purchase_price_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id
    ,o_raw.`table` AS event_table
    ,o_raw.type AS event_type
    ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
    ,cast(o_raw.ts AS BIGINT) AS event_date
    ,o_raw.goods_id
    ,o_raw.provider_id
    ,o_raw.dispatch_sn
    ,o_raw.price
    ,o_raw.wrap_price
    ,o_raw.is_delete
    ,f(o_raw.ctime != "0000-00-00 00:00:00" or o_raw.ctime is not null, unix_timestamp(o_raw.ctime, "yyyy-MM-dd HH:mm:ss"),0) AS ctime
    ,o_raw.pk_cat_id
    ,o_raw.last_purchase_price
    ,o_raw.last_purchase_wrapprice
    ,o_raw.last_purchase_provider
    ,o_raw.provider_id2
    ,o_raw.provider_id3
    ,o_raw.provider_id4
    ,o_raw.provider_id5
    ,o_raw.ratio
    ,o_raw.ratio2
    ,o_raw.ratio3
    ,o_raw.ratio4
    ,o_raw.ratio5
    ,o_raw.color
    ,o_raw.color2
    ,o_raw.color3
    ,o_raw.color4
    ,o_raw.color5
    ,hour as hour
from tmp.tmp_fd_romeo_goods_purchase_price
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'goods_id', 'provider_id', 'dispatch_sn', 'price', 'wrap_price', 'is_delete', 'ctime', 'pk_cat_id', 'last_purchase_price', 'last_purchase_wrapprice', 'last_purchase_provider', 'provider_id2', 'provider_id3', 'provider_id4', 'provider_id5', 'ratio', 'ratio2', 'ratio3', 'ratio4', 'ratio5', 'color', 'color2', 'color3', 'color4', 'color5') o_raw
AS `table`, ts, `commit`, xid, type, old, goods_id, provider_id, dispatch_sn, price, wrap_price, is_delete, ctime, pk_cat_id, last_purchase_price, last_purchase_wrapprice, last_purchase_provider, provider_id2, provider_id3, provider_id4, provider_id5, ratio, ratio2, ratio3, ratio4, ratio5, color, color2, color3, color4, color5
where dt = '${hiveconf:dt}';
