CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_user_agent_analysis_inc (
`user_agent_id` bigint COMMENT '自增id',
`is_app` int COMMENT '是否是app',
`device_type` string COMMENT '设备类型',
`os_type` string COMMENT '操作系统类型',
`version` string COMMENT 'app版本号',
`device_id` string COMMENT '',
`uuid` string COMMENT 'uuid',
`project_name` string COMMENT '组织名',
`device_name` string COMMENT '设备详细名称',
`browser` string COMMENT '浏览器类型',
`idfa` string COMMENT 'idfa',
`idfv` string COMMENT 'idfv',
`imei` string COMMENT 'imei',
`android_id` string COMMENT 'android_id',
`ga_id` string COMMENT 'Google Advertising ID'
) COMMENT 'user agent分析表'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT OVERWRITE TABLE ods_fd_vb.ods_fd_user_agent_analysis_inc PARTITION (dt='${hiveconf:dt}')
select
        user_agent_id,
        is_app,
        device_type,
        os_type,
        version,
        device_id,
        uuid,
        project_name,
        device_name,
        browser,
        idfa,
        idfv,
        imei,
        android_id,
        ga_id
from tmp.tmp_fd_user_agent_analysis where dt = '${hiveconf:dt}';
