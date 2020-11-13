CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_email_subscribe(
        es_id int,
        email string ,
        create_timestamp bigint ,
        ip bigint ,
        source string   COMMENT 'Email来源',
        country_id int  COMMENT '国家region_id',
        project_name string   COMMENT '按项目退订',
        pn_bak string  ,
        language_id int
) comment '从tmp.tmp_email_subscribe同步过来的email_subscribe表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_email_subscribe
select `(dt)?+.+`
from ods_fd_vb.ods_fd_email_subscribe_arc
where dt = '${hiveconf:dt}';
