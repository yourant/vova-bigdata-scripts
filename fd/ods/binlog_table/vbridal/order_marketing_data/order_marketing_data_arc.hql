CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_marketing_data_arc
(
    id BIGINT,
    order_id BIGINT,
    sp_session_id STRING COMMENT 'artemis session_id',
    created_time STRING COMMENT '创建时间',
    last_update_time timestamp COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单session关联表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_vb.ods_fd_order_marketing_data_arc PARTITION (pt='${hiveconf:pt}')
select id, order_id, sp_session_id, created_time, last_update_time
from (
    select pt,id, order_id, sp_session_id, created_time, last_update_time,
       row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (
        select 
            pt,
            id,
            order_id,
            sp_session_id,
            created_time,
            last_update_time
        from ods_fd_vb.ods_fd_order_marketing_data_arc where pt='${hiveconf:pt_last}'
        UNION
        select
            pt,
            id,
            order_id,
            sp_session_id,
            created_time,
            last_update_time
        from ods_fd_vb.ods_fd_order_marketing_data_inc where pt >= '${hiveconf:pt}'
    ) arc
)tab where tab.rank = 1;
