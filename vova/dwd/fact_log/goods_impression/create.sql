drop table IF EXISTS dwd.dwd_vova_fact_log_goods_impression;
CREATE  TABLE IF NOT EXISTS dwd.dwd_vova_fact_log_goods_impression (
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
list_uri string                comment '列表链接',
list_type string               comment '列表类型',
virtual_goods_id bigint        comment '商品虚拟id',
absolute_position string       comment '绝对位置',
recall_pool string             comment '召回池',
activity_code string           comment '活动标签',
activity_detail string         comment '活动标签详情',
session_id string              comment 'session_id',
app_uri  string                comment 'app_uri',
element_type  string           comment '元素类型',
landing_page  string           comment 'landing_page',
imsi  string           comment 'imsi'
)partitioned BY (pt string) row format delimited fields terminated by '\001' stored as parquetfile;