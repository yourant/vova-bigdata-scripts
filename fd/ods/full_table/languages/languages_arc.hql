CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_languages_arc(
  `languages_id` int COMMENT '自增id',
  `name` string COMMENT '',
  `code` string COMMENT '使用zh-CN格式',
  `disabled` int COMMENT '0启用；1禁用',
  `lang_order` int COMMENT '',
  `last_update_time` string COMMENT '最后更新时间'
 )comment '币种'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");



INSERT overwrite table ods_fd_vb.ods_fd_languages_arc PARTITION (dt='${hiveconf:dt}')
select  
    languages_id,
    name,
    code,
    disabled,
    lang_order,
    last_update_time
from tmp.tmp_fd_languages_full;
