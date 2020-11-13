CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_price_adjust_log(
  id int COMMENT '主键'
  ,worker string COMMENT '操作人'
  ,act_id int COMMENT '调价申请主键'
  ,act_table string COMMENT '操作表'
  ,act_content string COMMENT '操作内容'
  ,note string COMMENT '备注'
  ,created bigint COMMENT '创建时间'
 )comment '申请调价核价操作历史表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods_price_adjust_log
select `(dt)?+.+` from ods_fd_vb.ods_fd_goods_price_adjust_log_arc where dt = '${hiveconf:dt}';
