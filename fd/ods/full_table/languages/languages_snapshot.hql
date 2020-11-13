CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_languages(
  `languages_id` int COMMENT '自增id',
  `name` string COMMENT '',
  `code` string COMMENT '使用zh-CN格式',
  `disabled` int COMMENT '0启用；1禁用',
  `lang_order` int COMMENT '',
  `last_update_time` string COMMENT '最后更新时间'
 )comment '币种'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_languages
select `(dt)?+.+` 
from ods_fd_vb.ods_fd_languages_arc
where dt = '${hiveconf:dt}';
