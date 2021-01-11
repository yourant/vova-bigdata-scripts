drop external table IF EXISTS dwd.dwd_vova_log_common_click_history;
CREATE external TABLE IF NOT EXISTS dwd.dwd_vova_log_common_click_history (
event_fingerprint string       comment '事件唯一标识',
datasource       string       comment '事件来源，vova|ac',
event_name        string       comment '事件名',
platform          string       comment '平台，web|mob',
collector_tstamp  bigint       comment '事件在服务端的收集时间戳',
dvce_created_tstamp bigint     comment '事件创建的时间戳',
derived_tstamp bigint          comment '经过修复的客户端事件时间戳',
name_tracker  string           comment '',
buyer_id  bigint               comment '用户id',
domain_userid string           comment '',
language string                comment '语言',
country string                 comment '国家',
geo_country string             comment 'snowplow上传国家',
geo_city string                comment 'snowplow上传城市',
currency string                comment 'snowplow上传国家',
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
landing_page    string         comment '',
imsi    string	               comment  ''
)partitioned BY (pt string) row format delimited fields terminated by '\001' stored as parquetfile;



CREATE external TABLE `dwd`.`dwd_vova_log_data_history`(`event_fingerprint` STRING COMMENT '??????', `datasource` STRING COMMENT '?????vova|ac', `event_name` STRING COMMENT '???', `platform` STRING COMMENT '???web|mob', `collector_tstamp` STRING COMMENT '????????????', `dvce_created_tstamp` STRING COMMENT '????????', `derived_tstamp` STRING COMMENT '?????????????', `name_tracker` STRING COMMENT '', `buyer_id` BIGINT COMMENT '??id', `domain_userid` STRING COMMENT '', `language` STRING COMMENT '??', `country` STRING COMMENT '??', `geo_country` STRING COMMENT 'snowplow????', `geo_city` STRING COMMENT 'snowplow????', `currency` STRING COMMENT 'snowplow????', `page_code` STRING COMMENT '??', `gender` STRING COMMENT '??', `page_url` STRING COMMENT '????', `session_id` STRING COMMENT 'session_id', `app_uri` STRING COMMENT 'app_uri', `account_class` STRING COMMENT '', `channel_type` STRING COMMENT '', `app_version` STRING COMMENT 'app???', `device_model` STRING COMMENT '????', `device_id` STRING COMMENT '??id', `referrer` STRING COMMENT '', `organic_idfv` STRING COMMENT '', `advertising_id` STRING COMMENT '', `advertising_id_sp` STRING COMMENT '', `test_info` STRING COMMENT '????', `media_source` STRING COMMENT '', `sys_lang` STRING COMMENT '????', `sys_country` STRING COMMENT '????', `vpn` STRING COMMENT '????vpn', `email` STRING COMMENT '??', `latlng` STRING COMMENT '', `root` STRING COMMENT '', `is_tablet` STRING COMMENT '?????', `os_type` STRING COMMENT '????,ios|android', `os_version` STRING COMMENT '?????', `ip` STRING COMMENT 'ip??', `element_name` STRING COMMENT '????', `extra` STRING COMMENT '????', `element_id` STRING, `landing_page` STRING, `imsi` STRING)
PARTITIONED BY (`pt` STRING) row format delimited fields terminated by '\001' stored as parquetfile;


CREATE external TABLE `dwd`.`dwd_vova_log_goods_click_history`(`event_fingerprint` STRING COMMENT '??????', `datasource` STRING COMMENT '?????vova|ac', `event_name` STRING COMMENT '???', `platform` STRING COMMENT '???web|mob', `collector_tstamp` BIGINT COMMENT '????????????', `dvce_created_tstamp` BIGINT COMMENT '????????', `derived_tstamp` BIGINT COMMENT '?????????????', `name_tracker` STRING COMMENT '', `buyer_id` BIGINT COMMENT '??id', `domain_userid` STRING COMMENT '', `language` STRING COMMENT '??', `country` STRING COMMENT '??', `geo_country` STRING COMMENT 'snowplow????', `geo_city` STRING COMMENT 'snowplow????', `currency` STRING COMMENT 'snowplow????', `page_code` STRING COMMENT '??', `gender` STRING COMMENT '??', `page_url` STRING COMMENT '????', `account_class` STRING COMMENT '', `channel_type` STRING COMMENT '', `view_type` STRING COMMENT '????,show|hide', `app_version` STRING COMMENT 'app???', `device_model` STRING COMMENT '????', `device_id` STRING COMMENT '??id', `referrer` STRING COMMENT '', `organic_idfv` STRING COMMENT '', `advertising_id` STRING COMMENT '', `advertising_id_sp` STRING COMMENT '', `test_info` STRING COMMENT '????', `media_source` STRING COMMENT '', `sys_lang` STRING COMMENT '????', `sys_country` STRING COMMENT '????', `vpn` STRING COMMENT '????vpn', `email` STRING COMMENT '??', `latlng` STRING COMMENT '', `root` STRING COMMENT '', `is_tablet` STRING COMMENT '?????', `os_type` STRING COMMENT '????,ios|android', `os_version` STRING COMMENT '?????', `ip` STRING COMMENT 'ip??', `list_uri` STRING COMMENT '????', `list_type` STRING COMMENT '????', `virtual_goods_id` BIGINT COMMENT '????id', `absolute_position` STRING COMMENT '????', `element_url` STRING COMMENT '??url', `recall_pool` STRING COMMENT '???', `activity_code` STRING COMMENT '????', `activity_detail` STRING COMMENT '??????', `session_id` STRING COMMENT 'session_id', `app_uri` STRING COMMENT 'app_uri', `element_type` STRING, `landing_page` STRING, `imsi` STRING)
PARTITIONED BY (`pt` STRING) row format delimited fields terminated by '\001' stored as parquetfile;

CREATE external TABLE `dwd`.`dwd_vova_log_goods_impression_history`(`event_fingerprint` STRING COMMENT '??????', `datasource` STRING COMMENT '?????vova|ac', `event_name` STRING COMMENT '???', `platform` STRING COMMENT '???web|mob', `collector_tstamp` BIGINT COMMENT '????????????', `dvce_created_tstamp` BIGINT COMMENT '????????', `derived_tstamp` BIGINT COMMENT '?????????????', `name_tracker` STRING COMMENT '', `buyer_id` BIGINT COMMENT '??id', `domain_userid` STRING COMMENT '', `language` STRING COMMENT '??', `country` STRING COMMENT '??', `geo_country` STRING COMMENT 'snowplow????', `geo_city` STRING COMMENT 'snowplow????', `currency` STRING COMMENT 'snowplow????', `page_code` STRING COMMENT '??', `gender` STRING COMMENT '??', `page_url` STRING COMMENT '????', `account_class` STRING COMMENT '', `channel_type` STRING COMMENT '', `view_type` STRING COMMENT '????,show|hide', `app_version` STRING COMMENT 'app???', `device_model` STRING COMMENT '????', `device_id` STRING COMMENT '??id', `referrer` STRING COMMENT '', `organic_idfv` STRING COMMENT '', `advertising_id` STRING COMMENT '', `advertising_id_sp` STRING COMMENT '', `test_info` STRING COMMENT '????', `media_source` STRING COMMENT '', `sys_lang` STRING COMMENT '????', `sys_country` STRING COMMENT '????', `vpn` STRING COMMENT '????vpn', `email` STRING COMMENT '??', `latlng` STRING COMMENT '', `root` STRING COMMENT '', `is_tablet` STRING COMMENT '?????', `os_type` STRING COMMENT '????,ios|android', `os_version` STRING COMMENT '?????', `ip` STRING COMMENT 'ip??', `list_uri` STRING COMMENT '????', `list_type` STRING COMMENT '????', `virtual_goods_id` BIGINT COMMENT '????id', `absolute_position` STRING COMMENT '????', `recall_pool` STRING COMMENT '???', `activity_code` STRING COMMENT '????', `activity_detail` STRING COMMENT '??????', `session_id` STRING COMMENT 'session_id', `app_uri` STRING COMMENT 'app_uri', `element_type` STRING, `landing_page` STRING, `imsi` STRING)
PARTITIONED BY (`pt` STRING) row format delimited fields terminated by '\001' stored as parquetfile;


CREATE external TABLE `dwd`.`dwd_vova_log_impressions_history`(`event_fingerprint` STRING COMMENT '??????', `datasource` STRING COMMENT '?????vova|ac', `event_name` STRING COMMENT '???', `platform` STRING COMMENT '???web|mob', `collector_tstamp` STRING COMMENT '????????????', `dvce_created_tstamp` STRING COMMENT '????????', `derived_tstamp` STRING COMMENT '?????????????', `name_tracker` STRING COMMENT '', `buyer_id` BIGINT COMMENT '??id', `domain_userid` STRING COMMENT '', `language` STRING COMMENT '??', `country` STRING COMMENT '??', `geo_country` STRING COMMENT 'snowplow????', `geo_city` STRING COMMENT 'snowplow????', `currency` STRING COMMENT 'snowplow????', `page_code` STRING COMMENT '??', `gender` STRING COMMENT '??', `page_url` STRING COMMENT '????', `account_class` STRING COMMENT '', `channel_type` STRING COMMENT '', `app_version` STRING COMMENT 'app???', `device_model` STRING COMMENT '????', `device_id` STRING COMMENT '??id', `referrer` STRING COMMENT '', `organic_idfv` STRING COMMENT '', `advertising_id` STRING COMMENT '', `advertising_id_sp` STRING COMMENT '', `test_info` STRING COMMENT '????', `media_source` STRING COMMENT '', `sys_lang` STRING COMMENT '????', `sys_country` STRING COMMENT '????', `vpn` STRING COMMENT '????vpn', `email` STRING COMMENT '??', `latlng` STRING COMMENT '', `root` STRING COMMENT '', `is_tablet` STRING COMMENT '?????', `os_type` STRING COMMENT '????,ios|android', `os_version` STRING COMMENT '?????', `ip` STRING COMMENT 'ip', `session_id` STRING COMMENT 'session_id', `app_uri` STRING COMMENT 'app_uri', `list_type` STRING COMMENT 'list_type', `element_id` STRING COMMENT '??id', `element_name` STRING COMMENT '????', `element_type` STRING COMMENT '????', `element_position` STRING COMMENT '????', `extra` STRING COMMENT '????', `landing_page` STRING, `imsi` STRING)
PARTITIONED BY (`pt` STRING) row format delimited fields terminated by '\001' stored as parquetfile;


CREATE external TABLE `dwd`.`dwd_vova_log_order_process_history`(`event_fingerprint` STRING COMMENT '??????', `datasource` STRING COMMENT '?????vova|ac', `event_name` STRING COMMENT '???', `platform` STRING COMMENT '???web|mob', `collector_tstamp` BIGINT COMMENT '????????????', `dvce_created_tstamp` BIGINT COMMENT '????????', `derived_tstamp` BIGINT COMMENT '?????????????', `name_tracker` STRING COMMENT '', `buyer_id` BIGINT COMMENT '??id', `domain_userid` STRING COMMENT '', `language` STRING COMMENT '??', `country` STRING COMMENT '??', `geo_country` STRING COMMENT 'snowplow????', `geo_city` STRING COMMENT 'snowplow????', `currency` STRING COMMENT 'snowplow????', `page_code` STRING COMMENT '??', `gender` STRING COMMENT '??', `page_url` STRING COMMENT '????', `account_class` STRING COMMENT '', `channel_type` STRING COMMENT '', `view_type` STRING COMMENT '????,show|hide', `app_version` STRING COMMENT 'app???', `device_model` STRING COMMENT '????', `device_id` STRING COMMENT '??id', `referrer` STRING COMMENT '', `organic_idfv` STRING COMMENT '', `advertising_id` STRING COMMENT '', `advertising_id_sp` STRING COMMENT '', `test_info` STRING COMMENT '????', `media_source` STRING COMMENT '', `sys_lang` STRING COMMENT '????', `sys_country` STRING COMMENT '????', `vpn` STRING COMMENT '????vpn', `email` STRING COMMENT '??', `latlng` STRING COMMENT '', `root` STRING COMMENT '', `is_tablet` STRING COMMENT '?????', `os_type` STRING COMMENT '????,ios|android', `os_version` STRING COMMENT '?????', `ip` STRING COMMENT 'ip??', `element_name` STRING COMMENT '????', `submit_result` STRING COMMENT '????', `virtual_goods_id` BIGINT COMMENT '????id', `payment_method` STRING COMMENT '????', `activity_code` STRING COMMENT '????', `activity_detail` STRING COMMENT '??????', `session_id` STRING COMMENT 'session_id', `app_uri` STRING COMMENT 'app_uri', `landing_page` STRING, `imsi` STRING)
PARTITIONED BY (`pt` STRING) row format delimited fields terminated by '\001' stored as parquetfile;


CREATE external TABLE `dwd`.`dwd_vova_log_page_view_history`(`event_fingerprint` STRING COMMENT '??????', `datasource` STRING COMMENT '?????vova|ac', `event_name` STRING COMMENT '???', `platform` STRING COMMENT '???web|mob', `collector_tstamp` BIGINT COMMENT '????????????', `dvce_created_tstamp` BIGINT COMMENT '????????', `derived_tstamp` BIGINT COMMENT '?????????????', `name_tracker` STRING COMMENT '', `buyer_id` BIGINT COMMENT '??id', `domain_userid` STRING COMMENT '', `language` STRING COMMENT '??', `country` STRING COMMENT '??', `geo_country` STRING COMMENT 'snowplow????', `geo_city` STRING COMMENT 'snowplow????', `currency` STRING COMMENT 'snowplow????', `page_code` STRING COMMENT '??', `gender` STRING COMMENT '??', `page_url` STRING COMMENT '????', `account_class` STRING COMMENT '', `channel_type` STRING COMMENT '', `view_type` STRING COMMENT '????,show|hide', `app_version` STRING COMMENT 'app???', `device_model` STRING COMMENT '????', `device_id` STRING COMMENT '??id', `referrer` STRING COMMENT '', `organic_idfv` STRING COMMENT '', `advertising_id` STRING COMMENT '', `advertising_id_sp` STRING COMMENT '', `test_info` STRING COMMENT '????', `media_source` STRING COMMENT '', `sys_lang` STRING COMMENT '????', `sys_country` STRING COMMENT '????', `vpn` STRING COMMENT '????vpn', `email` STRING COMMENT '??', `latlng` STRING COMMENT '', `root` STRING COMMENT '', `is_tablet` STRING COMMENT '?????', `os_type` STRING COMMENT '????,ios|android', `os_version` STRING COMMENT '?????', `ip` STRING COMMENT 'ip??', `app_uri` STRING COMMENT 'app_uri', `activity_code` STRING COMMENT '????', `activity_detail` STRING COMMENT '??????', `session_id` STRING COMMENT 'session_id', `virtual_goods_id` BIGINT COMMENT '??????id', `landing_page` STRING, `imsi` STRING)
PARTITIONED BY (`pt` STRING) row format delimited fields terminated by '\001' stored as parquetfile;


CREATE external TABLE `dwd`.`dwd_vova_log_screen_view_history`(`event_fingerprint` STRING COMMENT '??????', `datasource` STRING COMMENT '?????vova|ac', `event_name` STRING COMMENT '???', `platform` STRING COMMENT '???web|mob', `collector_tstamp` BIGINT COMMENT '????????????', `dvce_created_tstamp` BIGINT COMMENT '????????', `derived_tstamp` BIGINT COMMENT '?????????????', `name_tracker` STRING COMMENT '', `buyer_id` BIGINT COMMENT '??id', `domain_userid` STRING COMMENT '', `language` STRING COMMENT '??', `country` STRING COMMENT '??', `geo_country` STRING COMMENT 'snowplow????', `geo_city` STRING COMMENT 'snowplow????', `currency` STRING COMMENT 'snowplow????', `page_code` STRING COMMENT '??', `gender` STRING COMMENT '??', `page_url` STRING COMMENT '????', `account_class` STRING COMMENT '', `channel_type` STRING COMMENT '', `view_type` STRING COMMENT '????,show|hide', `app_version` STRING COMMENT 'app???', `device_model` STRING COMMENT '????', `device_id` STRING COMMENT '??id', `referrer` STRING COMMENT '', `organic_idfv` STRING COMMENT '', `advertising_id` STRING COMMENT '', `advertising_id_sp` STRING COMMENT '', `test_info` STRING COMMENT '????', `media_source` STRING COMMENT '', `sys_lang` STRING COMMENT '????', `sys_country` STRING COMMENT '????', `vpn` STRING COMMENT '????vpn', `email` STRING COMMENT '??', `latlng` STRING COMMENT '', `root` STRING COMMENT '', `is_tablet` STRING COMMENT '?????', `os_type` STRING COMMENT '????,ios|android', `os_version` STRING COMMENT '?????', `ip` STRING COMMENT 'ip??', `activity_code` STRING COMMENT '????', `activity_detail` STRING COMMENT '??????', `session_id` STRING COMMENT 'session_id', `virtual_goods_id` BIGINT COMMENT '??????id', `app_uri` STRING COMMENT 'app_uri', `landing_page` STRING, `imsi` STRING)
PARTITIONED BY (`pt` STRING) row format delimited fields terminated by '\001' stored as parquetfile;