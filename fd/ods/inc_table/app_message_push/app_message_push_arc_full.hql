CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_app_message_push_arc
(
`id` bigint COMMENT '自增id',
`install_record_id` bigint COMMENT '用于关联app_install_record表',
`notice_id` string COMMENT '推送id',
`user_id` bigint COMMENT '用户id',
`event_type` bigint COMMENT '推送用户群组id',
`init_time` bigint COMMENT '记录插入时间',
`push_result` bigint COMMENT '发送结果标识',
`push_time` bigint COMMENT '推送时间'
) COMMENT 'app message 推送信息log表'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

INSERT overwrite table ods_fd_vb.ods_fd_app_message_push_arc PARTITION (dt='${hiveconf:dt}')
select id, install_record_id, notice_id, user_id, event_type, init_time, push_result, push_time
from (
    select id,install_record_id, notice_id, user_id, event_type, init_time, push_result, push_time, 
        row_number () OVER (PARTITION BY id ORDER BY dt DESC) AS rank
    from(
        select 
            '2020-01-01' as dt,
            id,
            install_record_id,
            notice_id,
            user_id,
            event_type,
            if(init_time != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(init_time, 'America/Los_Angeles'), "yyyy-MM-dd HH:mm:ss"), 0) as init_time,
            push_result,
            if(push_time != "0000-00-00 00:00:00", unix_timestamp(to_utc_timestamp(push_time, 'America/Los_Angeles'), "yyyy-MM-dd HH:mm:ss"), 0) as push_time
        from tmp.tmp_fd_app_message_push_full

        UNION

        select 
            '${hiveconf:dt}' as dt,
            id,
            install_record_id,
            notice_id,
            user_id,
            event_type,
            init_time,
            push_result,
            push_time
        from ods_fd_vb.ods_fd_app_message_push_inc where dt = '${hiveconf:dt}'
    )inc
) arc where arc.rank =1;
