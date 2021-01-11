drop table IF EXISTS dwd.dwd_vova_log_impressions;
CREATE external TABLE dwd.dwd_vova_log_impressions(
  datasource          string comment '事件来源，vova/ac',
  event_fingerprint   STRING COMMENT '事件唯一标识',
  event_name          STRING COMMENT '事件名',
  platform            STRING COMMENT '平台，web|mob',
  collector_tstamp    bigint COMMENT '事件在服务端的收集时间戳',
  dvce_created_tstamp bigint COMMENT '事件创建的时间戳',
  derived_tstamp      bigint COMMENT '经过修复的客户端事件时间戳',
  collector_ts        string COMMENT '事件在服务端的收集时间戳(如:2020-12-26 00:02:14)',
  dvce_created_ts     string COMMENT '事件创建的时间戳(如:2020-12-26 00:02:14)',
  name_tracker        STRING COMMENT '',
  buyer_id            BIGINT COMMENT '用户id',
  domain_userid       STRING COMMENT '',
  language            STRING COMMENT '语言',
  country             STRING COMMENT '国家',
  geo_country         STRING COMMENT 'snowplow上传国家',
  geo_city            STRING COMMENT 'snowplow上传城市',
  geo_region          string COMMENT 'snowplow上传国家和地区的代码ISO-3166-2',
  geo_latitude        string COMMENT 'snowplow上传位置纬度',
  geo_longitude       string COMMENT 'snowplow上传位置经度',
  geo_region_name     string COMMENT 'snowplow上传地区名称',
  geo_timezone        string COMMENT 'snowplow上传时区名称',
  currency            STRING COMMENT '货币类型',
  page_code           STRING COMMENT '页面',
  gender              STRING COMMENT '性别',
  page_url            STRING COMMENT '页面链接',
  account_class       STRING COMMENT '',
  channel_type        STRING COMMENT '',
  app_version         STRING COMMENT 'app版本号',
  device_model        STRING COMMENT '设备型号',
  device_id           STRING COMMENT '设备id',
  referrer            STRING COMMENT '',
  organic_idfv        STRING COMMENT '',
  advertising_id      STRING COMMENT '',
  advertising_id_sp   STRING COMMENT '',
  test_info           STRING COMMENT '测试信息',
  media_source        STRING COMMENT '',
  sys_lang            STRING COMMENT '系统语言',
  sys_country         STRING COMMENT '系统国家',
  vpn                 STRING COMMENT '是否启用vpn',
  email               STRING COMMENT '邮箱',
  latlng              STRING COMMENT '',
  root                STRING COMMENT '',
  is_tablet           STRING COMMENT '是否是平板',
  os_type             STRING COMMENT '系统类型,ios|android',
  os_version          STRING COMMENT '系统版本号',
  ip                  STRING COMMENT 'ip',
  session_id          STRING COMMENT 'session_id',
  app_uri             STRING COMMENT 'app_uri',
  list_type           STRING COMMENT 'list_type',
  element_id          STRING COMMENT '元素id',
  element_name        STRING COMMENT '元素名称',
  element_type        STRING COMMENT '元素类型',
  element_position    STRING COMMENT '元素位置',
  extra               STRING COMMENT '额外信息',
  landing_page        STRING,
  imsi                STRING,
  br_family           string COMMENT 'Browser family No Firefox',
  br_version          string COMMENT 'Browser version No 12.0'
)
COMMENT '每日全量非商品曝光'
PARTITIONED BY (pt string, dp string)
row format delimited fields terminated by '\001' stored as parquetfile
LOCATION "s3://bigdata-offline/warehouse/dwd/dwd_vova_log_impressions/"
;