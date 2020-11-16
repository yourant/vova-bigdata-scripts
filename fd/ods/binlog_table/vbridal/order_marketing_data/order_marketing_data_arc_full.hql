CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_marketing_data_arc (
    id BIGINT,
    order_id BIGINT,
    sp_session_id STRING COMMENT 'artemis session_id',
    created_time STRING COMMENT '创建时间',
    last_update_time BIGINT COMMENT '最后更新时间'
) COMMENT 'kafka同步过来的数据库订单session关联表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

INSERT overwrite table ods_fd_vb.ods_fd_order_marketing_data_arc PARTITION (pt='${hiveconf:pt}')
select id, order_id, sp_session_id, created_time, last_update_time 
from (
    select pt,id, order_id, sp_session_id, created_time, last_update_time,
       row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (
        select 
            '2020-01-01' as pt,
            id,
            order_id,
            sp_session_id,
            created_time,
            cast(unix_timestamp(last_update_time, 'yyyy-MM-dd HH:mm:ss') as BIGINT)  last_update_time
        from tmp.tmp_fd_order_marketing_data_full
        UNION
        select pt, id, order_id, sp_session_id, created_time, last_update_time
        from (
            select 
                '${hiveconf:pt}' as pt,
                id,
                order_id,
                sp_session_id,
                created_time,
                last_update_time,
                row_number () OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_vb.ods_fd_order_marketing_data_inc where pt = '${hiveconf:pt_format}'
        )inc where inc.rank = 1
    ) arc
)tab where tab.rank = 1;
