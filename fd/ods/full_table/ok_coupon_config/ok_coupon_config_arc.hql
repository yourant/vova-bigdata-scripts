CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_ok_coupon_config_arc
(
  `coupon_config_id` int COMMENT '',
  `site_id` int COMMENT '启用端编号',
  `party_id` int,
  `coupon_config_value` double COMMENT '红包的价值',
  `coupon_config_status` int COMMENT '红包的使用状态：0 启用；1 禁用',
  `coupon_config_stime` int COMMENT '红包启用的开始时间, start time',
  `coupon_config_etime` int COMMENT '红包启用的结束时间, end time',
  `coupon_config_comment` string COMMENT '红包类型注释',
  `coupon_config_coupon_type` string COMMENT '是值还是百分比',
  `coupon_config_apply_type` string COMMENT '是针对商品价格还是针对运费',
  `coupon_config_data` string COMMENT '红包配置',
  `coupon_config_type_id` int COMMENT '红包的类型（促销，返现，补偿）',
  `coupon_config_minimum_amount` double COMMENT '超过此值方可使用该优惠券',
  `cat_id` string COMMENT '跟分类关联',
  `goods_id` string,
  `goods_attr` string COMMENT '商品属性',
  `platform` string COMMENT '跟平台关联(PC,H5,APP)',
  `region_code` string COMMENT '跟国家关联',
  `created_by` int COMMENT '创建人',
  `created_datetime` string COMMENT '创建时间',
  `currency_id` int COMMENT '币种ID',
  `currency` string COMMENT 'USD HKD',
  `coupon_config_minimum_goods_number` int
 )comment '红包'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


INSERT overwrite table ods_fd_vb.ods_fd_ok_coupon_config_arc PARTITION (dt='${hiveconf:dt}')
select  
coupon_config_id,
site_id,
party_id,
coupon_config_value,
coupon_config_status,
coupon_config_stime,
coupon_config_etime,
coupon_config_comment,
coupon_config_coupon_type,
coupon_config_apply_type,
coupon_config_data,
coupon_config_type_id,
coupon_config_minimum_amount,
cat_id,
goods_id,
goods_attr,
platform,
region_code,
created_by,
created_datetime,
currency_id,
currency,
coupon_config_minimum_goods_number
from tmp.tmp_fd_ok_coupon_config_full;
