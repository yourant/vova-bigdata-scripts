CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_category(
  `cat_id` int,
  `cat_name` string,
  `cat_goods_name` string,
  `depth` int,
  `keywords` string,
  `cat_desc` string,
  `parent_id` int,
  `sort_order` int,
  `is_show` TINYINT,
  `party_id` int,
  `config` string,
  `erp_cat_id` int,
  `erp_top_cat_id` int,
  `pk_cat_id` int,
  `is_accessory` TINYINT,
  `last_update_time` string
 )comment '商品类目信息'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_category
select `(dt)?+.+` 
from ods_fd_vb.ods_fd_category_arc 
where dt = '${hiveconf:dt}';
