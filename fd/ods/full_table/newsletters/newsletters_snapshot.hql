CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_newsletters(
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_newsletters
select `(dt)?+.+`
from ods_fd_vb.ods_fd_newsletters_arc
where dt = '${hiveconf:dt}';
