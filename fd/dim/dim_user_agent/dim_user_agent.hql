CREATE TABLE IF NOT EXISTS `dim.dim_fd_user_agent`(
  `user_agent_id` bigint COMMENT 'user agent id',
  `platform` string COMMENT '平台',
  `platform_type` string COMMENT '细分平台',
  `is_app` bigint COMMENT '是否APP',
  `device_type` string COMMENT '设备类型',
  `os_type` string COMMENT '操作系统类型',
  `version` string COMMENT 'APP版本号',
  `device_id` string COMMENT '设备id',
  `uuid` string COMMENT 'UUID',
  `device_name` string COMMENT '设备名称',
  `browser` string COMMENT '浏览器',
  `idfa` string COMMENT 'idfa',
  `idfv` string COMMENT 'idfv',
  `imei` string COMMENT 'imei',
  `android_id` string COMMENT 'android id',
  `ga_id` string COMMENT 'Google Advertising ID'
)
COMMENT '用户使用平台信息维表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

INSERT overwrite table dim.dim_fd_user_agent
select 
    `user_agent_id`
    , case
          when is_app = 0 and device_type = 'pc' then 'PC'
          when is_app = 0 and device_type = 'mobile' then 'H5'
          when is_app = 0 and device_type = 'pad' then 'Tablet'
          when is_app = 1 and os_type = 'ios' then 'IOS'
          when is_app = 1 and os_type = 'android' then 'Android'
          else 'others'
    end           as platform
    , case
          when is_app = 0 and device_type = 'pc' then 'pc_web'
          when is_app = 0 and device_type = 'mobile' then 'mobile_web'
          when is_app = 0 and device_type = 'pad' then 'tablet_web'
          when is_app = 1 and os_type = 'ios' then 'ios_app'
          when is_app = 1 and os_type = 'android' then 'android_app'
          else 'others'
    end           as platform_type
    , `is_app`
    , `device_type`
    , `os_type`
    , `version`  as ua_version
    , `device_id`
    , `uuid`     as ua_uuid
    , `device_name`
    , `browser`
    , `idfa`
    , `idfv`
    , `imei`
    , `android_id`
    , `ga_id`
from ods_fd_vb.ods_fd_user_agent_analysis;
