CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_newsletters_arc(
        nl_id int,
        nl_code string  COMMENT 'newsletter code',
        nl_type string ,
        type string  COMMENT 'newsletter不同模式',
        email_body string  COMMENT '邮件正文',
        email_subject string  COMMENT '邮件标题',
        create_time bigint COMMENT '创建时间' ,
        start_time bigint  COMMENT '开始时间',
        send_time bigint ,
        send_count int  COMMENT '发送数量',
        open_count int  COMMENT '打开数量',
        arrive_count int  COMMENT '发送成功到达数量',
        visitors int COMMENT '访问人数',
        nl_status int  COMMENT 'nl状态',
        newsletter_url string ,
        thumbnail_url string  COMMENT '缩略图url',
        note string  COMMENT '一些备注信息',
        is_delete int   COMMENT '是否删除'
) comment '从tmp.tmp_newsletters同步过来的newsletters表'
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_newsletters_arc PARTITION (dt='${hiveconf:dt}')
select
    	nl_id
	,nl_code
	,nl_type
	,type
	,email_body
	,email_subject
	,if(create_time = '0000-00-00 00:00:00',0,unix_timestamp(create_time)) as create_time
	,if(start_time = '0000-00-00 00:00:00',0,unix_timestamp(start_time)) as start_time
	,if(send_time = '0000-00-00 00:00:00',0,unix_timestamp(send_time)) as send_time
	,send_count
	,open_count
	,arrive_count
	,visitors
	,nl_status
	,newsletter_url
	,thumbnail_url
	,note
	,is_delete
from tmp.tmp_fd_newsletters_full;
