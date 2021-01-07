drop table IF EXISTS dwd.dwd_vova_log_common_click;
CREATE external TABLE dwd.dwd_vova_log_common_click(
  datasource          string     comment '事件来源，vova/ac',
  event_fingerprint   string     comment '事件唯一标识',
  event_name          string     comment '事件名',
  platform            string     comment '平台，web|mob',
  collector_tstamp      bigint   COMMENT '事件在服务端的收集时间戳',
  dvce_created_tstamp   bigint   COMMENT '事件创建的时间戳',
  derived_tstamp        bigint   COMMENT '经过修复的客户端事件时间戳',
  collector_ts          string   COMMENT '事件在服务端的收集时间戳(如:2020-12-26 00:02:14)',
  dvce_created_ts       string   COMMENT '事件创建的时间戳(如:2020-12-26 00:02:14)',
  name_tracker        string     comment '',
  buyer_id  bigint               comment '用户id',
  domain_userid string           comment '',
  language string                comment '语言',
  country string                 comment '国家',
  geo_country string             comment 'snowplow上传国家',
  geo_city string                comment 'snowplow上传城市',
  geo_region            string   COMMENT 'snowplow上传国家和地区的代码ISO-3166-2',
  geo_latitude          string   COMMENT 'snowplow上传位置纬度',
  geo_longitude         string   COMMENT 'snowplow上传位置经度',
  geo_region_name       string   COMMENT 'snowplow上传地区名称',
  geo_timezone          string   COMMENT 'snowplow上传时区名称',
  currency string                comment '货币类型',
  page_code string               comment '页面',
  gender string                  comment '性别',
  page_url string                comment '页面链接',
  account_class string           comment '',
  channel_type string            comment '',
  view_type string               comment '浏览类型,show|hide',
  app_version string             comment 'app版本号',
  device_model string            comment '设备型号',
  device_id string               comment '设备id',
  referrer string                comment '',
  organic_idfv string            comment '',
  advertising_id string          comment '',
  advertising_id_sp string       comment '',
  test_info string               comment '测试信息',
  media_source string            comment '',
  sys_lang string                comment '系统语言',
  sys_country string             comment '系统国家',
  vpn string                     comment '是否启用vpn',
  email string                   comment '邮箱',
  latlng string                  comment '',
  root string                    comment '',
  is_tablet string               comment '是否是平板',
  os_type string                 comment '系统类型,ios|android',
  os_version string              comment '系统版本号',
  ip string                      comment 'ip地址',
  element_name string            comment '元素名称',
  element_url string             comment '元素url',
  element_content string         comment '元素内容',
  list_uri string                comment '列表连接',
  list_name string               comment '列表名称',
  element_id string              comment '元素id',
  element_type string            comment '元素类型',
  element_position string        comment '元素位置',
  activity_code string           comment '活动标签',
  activity_detail string         comment '活动详情',
  session_id string              comment 'session_id',
  app_uri  string                comment 'app_uri',
  landing_page  string           comment 'landing_page',
  imsi  string                   comment 'imsi',
  br_family           string     COMMENT 'Browser family No Firefox',
  br_version          string     COMMENT 'Browser version No 12.0'
)
COMMENT '点击日志(全部的 normal 的打点)'
PARTITIONED BY (pt string, dp string)
row format delimited fields terminated by '\001' stored as parquetfile
LOCATION "s3://bigdata-offline/warehouse/dwd/dwd_vova_log_common_click/"
;






