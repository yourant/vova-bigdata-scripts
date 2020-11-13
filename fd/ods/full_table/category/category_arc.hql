CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_category_arc
(
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
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

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
