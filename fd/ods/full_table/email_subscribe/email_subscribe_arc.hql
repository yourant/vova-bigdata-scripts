CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_email_subscribe_arc(
        es_id bigint,
        email string ,
        create_timestamp bigint ,
        ip bigint ,
        source string   COMMENT 'Email来源',
        country_id bigint  COMMENT '国家region_id',
        project_name string   COMMENT '按项目退订',
        pn_bak string  ,
        language_id bigint
) comment '从tmp.tmp_email_subscribe同步过来的email_subscribe表'
PARTITIONED BY (pt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


INSERT overwrite table ods_fd_vb.ods_fd_email_subscribe_arc PARTITION (pt='${hiveconf:pt}')
select
    es_id
    ,email
    ,if(create_time = '0000-00-00 00:00:00',0,unix_timestamp(create_time)) as create_time
    ,ip
    ,source
    ,country_id
    ,project_name
    ,pn_bak
    ,language_id
from tmp.tmp_fd_email_subscribe_full;
