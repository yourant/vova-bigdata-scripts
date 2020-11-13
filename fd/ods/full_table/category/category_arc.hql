CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_category_arc
(
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
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

INSERT overwrite table ods_fd_vb.ods_fd_category_arc PARTITION (dt='${hiveconf:dt}')
select  
    cat_id,
    cat_name,
    cat_goods_name,
    depth,
    keywords,
    cat_desc,
    parent_id,
    sort_order,
    is_show,
    party_id,
    config,
    erp_cat_id,
    erp_top_cat_id,
    pk_cat_id,
    is_accessory,
    last_update_time
from tmp.tmp_fd_category_full;
