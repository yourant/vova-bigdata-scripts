CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_marketing_data (
    id INT,
    order_id INT,
    sp_session_id STRING COMMENT 'artemis session_id',
    created_time STRING COMMENT '创建时间',
    last_update_time BIGINT COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单session关联表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_order_marketing_data 
select `(dt)?+.+` 
from ods_fd_vb.ods_fd_order_marketing_data_arc
where dt = '${hiveconf:dt}'
;
