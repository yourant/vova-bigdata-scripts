CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_extension (
    id bigint,
    order_id bigint,
    ext_name string,
    ext_value string,
    is_delete bigint,
    last_update_time timestamp COMMENT '最后更新时间'
) COMMENT '来自arc当天全量数据订单扩展表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_order_extension
select `(pt)?+.+`
from ods_fd_vb.ods_fd_order_extension_arc 
where pt >= '${hiveconf:pt}'
;

