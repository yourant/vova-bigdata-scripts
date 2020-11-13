CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_email_unsubscribe_arc(
  		  eu_id bigint ,
		  email string ,
		  type string   COMMENT '取消的渠道类型',
		  datetime_timestamp bigint ,
		  reason string  COMMENT '取消原因',
		  project_name string  COMMENT '按项目退订'
) comment '从tmp.tmp_email_unsubscribe同步过来的email_unsubscribe表'
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


INSERT overwrite table ods_fd_vb.ods_fd_email_unsubscribe_arc PARTITION (dt='${hiveconf:dt}')
select
   		   eu_id
		  ,email
		  ,type
		  ,if(datetime = '0000-00-00 00:00:00',0,unix_timestamp(datetime)) as datetime_time
		  ,reason
		  ,project_name
from tmp.tmp_fd_email_unsubscribe_full;
