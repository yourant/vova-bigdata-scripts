CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_email_unsubscribe(
        eu_id bigint ,
        email string ,
        type string   COMMENT '取消的渠道类型',
        datetime_timestamp bigint ,
        reason string  COMMENT '取消原因',
        project_name string  COMMENT '按项目退订'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_email_unsubscribe
select `(dt)?+.+`
from ods_fd_vb.ods_fd_email_unsubscribe_arc
where dt = '${hiveconf:dt}';
