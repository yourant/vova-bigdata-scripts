CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_extension (
    id INT,
    order_id INT,
    ext_name STRING,
    ext_value STRING,
    is_delete TINYINT,
    last_update_time BIGINT COMMENT '最后更新时间'
) COMMENT '来自arc当天全量数据订单扩展表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_order_extension
select `(dt)?+.+`
from ods_fd_vb.ods_fd_order_extension_arc 
where dt = '${hiveconf:dt}'
;

