CREATE TABLE IF NOT EXISTS dwb.dwb_fd_newsletter_send_rpt (
`project` string COMMENT '组织',
`nl_code_num` string COMMENT 'nl期数',
`nl_code` string COMMENT 'nl_code',
`nl_type` string COMMENT 'nl_type',
`create_time` string COMMENT '创建时间',
`send_time` string COMMENT '发送时间',
`total_count` bigint COMMENT '发送量',
`success_count` bigint COMMENT '成功量',
`fail_count` bigint COMMENT '失败量',
`open_count` bigint COMMENT '打开量',
`unsubscribe_count` bigint COMMENT '退订量'
) COMMENT 'Newsltter 发送量报表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");