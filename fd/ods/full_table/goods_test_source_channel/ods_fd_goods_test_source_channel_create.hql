create table if not exists ods_fd_vb.ods_fd_goods_test_source_channel(
source_channel                             string
) comment '测款商品渠道来源'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");