CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_category(
  `cat_id` bigint,
  `cat_name` string,
  `cat_goods_name` string,
  `depth` bigint,
  `keywords` string,
  `cat_desc` string,
  `parent_id` bigint,
  `sort_order` bigint,
  `is_show` bigint,
  `party_id` bigint,
  `config` string,
  `erp_cat_id` bigint,
  `erp_top_cat_id` bigint,
  `pk_cat_id` bigint,
  `is_accessory` bigint,
  `last_update_time` string
 )comment '商品类目信息'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_category
select `(pt)?+.+`
from ods_fd_vb.ods_fd_category_arc 
where dt = '${hiveconf:dt}';
