CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_price_adjust_log_arc
(
  id int COMMENT '主键'
  ,worker string COMMENT '操作人'
  ,act_id int COMMENT '调价申请主键'
  ,act_table string COMMENT '操作表'
  ,act_content string COMMENT '操作内容'
  ,note string COMMENT '备注'
  ,created bigint COMMENT '创建时间'
 )comment '申请调价核价操作历史表'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_goods_price_adjust_log_arc PARTITION (dt='${hiveconf:dt}')
select  
    id,
    worker,
    act_id,
    act_table,
    act_content,
    note,
    unix_timestamp(date_format(from_utc_timestamp(to_utc_timestamp(created,'America/Los_Angeles'),'UTC'),'yyyy-MM-dd HH:mm:ss')) as created
from tmp.tmp_fd_goods_price_adjust_log_full;
