CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_turntable_record_v2_arc(
   	`record_id` bigint COMMENT '', 
	`user_id` bigint COMMENT '', 
	`device_id` string COMMENT '设备id', 
	`winning_time` string COMMENT '获奖日期', 
	`user_name` string COMMENT '',
	`prize_type` string COMMENT '活动分类',
	`activity_name` string COMMENT '活动名',
	`prize_name` string COMMENT '奖品name',
	`coupon_code` string COMMENT '',
 	 `points` bigint COMMENT '奖励积分')
COMMENT '数据库同步过来的用户签到表'
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_turntable_record_v2_arc PARTITION (dt='${hiveconf:dt}')
select
    record_id,
    user_id,
    device_id,
    winning_time,
    user_name,
    prize_type,
    activity_name,
    prize_name,
    coupon_code,
    points
from tmp.tmp_fd_turntable_record_v2_full;
