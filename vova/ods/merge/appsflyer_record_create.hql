DROP TABLE ods_vova_vtlr.ods_vova_appsflyer_record_merge;
CREATE EXTERNAL TABLE ods_vova_vtlr.ods_vova_appsflyer_record_merge(
datasource      string COMMENT 'datasource',
device_id       string COMMENT '设备id',
media_source    string COMMENT '渠道',
platform        string COMMENT '平台',
idfv            string COMMENT 'idfv',
android_id      string COMMENT 'android_id',
imei            string COMMENT 'imei',
advertising_id  string COMMENT '营销专用',
http_referrer   string COMMENT '营销专用',
campaign        string COMMENT '营销专用',
os_version      string COMMENT '系统版本',
country_code    string COMMENT '国家code',
language        string COMMENT '语言',
install_time    timestamp COMMENT '安装时间',
app_version     string COMMENT 'app版本',
bundle_id       string COMMENT '站点绑定',
create_time     timestamp COMMENT '创建时间'
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
