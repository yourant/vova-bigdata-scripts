# ods.vova_app_push_logs_v3
# ods_vova_ext.ods_vova_app_push_logs_raw
CREATE EXTERNAL TABLE ods_vova_ext.ods_vova_app_push_logs_raw(
data_json string
) COMMENT '推送点击原始日志' PARTITIONED BY (pt string)
LOCATION "s3://bigdata-offline/warehouse/pdb/vova/vvqueue/vvqueue-push_log"
;

# ods.vova_app_push_logs_v4
# ods_vova_ext.ods_vova_app_push_logs
CREATE TABLE ods_vova_ext.ods_vova_app_push_logs(
install_record_id int    COMMENT '用于关联app_install_record表',
notice_id         string COMMENT 'notice_id',
platform          string COMMENT 'platform',
user_id           int    COMMENT 'user_id',
task_id           int    COMMENT '推送任务ID',
task_config_id    int    COMMENT 'task_config_id',
push_result       int    COMMENT '发送结果标识',
response_id       string COMMENT 'response_id',
switch_on         int    COMMENT '推送权限打开状态：0-关闭，1-打开',
push_time         string COMMENT '推送时间'
) COMMENT '推送点击日志' PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
;