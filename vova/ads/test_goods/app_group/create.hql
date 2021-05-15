-- app group商品测款
-- 需求链接 https://zt.gitvv.com/index.php?m=task&f=view&taskID=34280

CREATE EXTERNAL TABLE ads.ads_vova_app_group_test_goods(
  `datasource` string,
  `platform` string,
  `region_codes` string,
  `region_ids` string,
  `goods_id` int,
  `users` bigint,
  `clicks` bigint,
  `impressions` bigint,
  `sales_order` bigint,
  `is_compliance` bigint,
  `employee_name` string,
  `select_cat_channel` string,
  `gmv` decimal(13,2),
  `ctr` decimal(13,2),
  `gcr` decimal(13,2),
  `test_status` bigint,
  `test_result` bigint,
  `create_time` timestamp,
  `last_update_time` string) PARTITIONED BY ( pt string) STORED AS PARQUETFILE;

CREATE EXTERNAL TABLE ads.ads_vova_app_group_cat_gcr(
is_brand          bigint comment '是否品牌',
first_cat_id      bigint comment '一级品类',
second_cat_id     bigint comment '二级品类',
gcr decimal(13,2) comment 'gcr') STORED AS PARQUETFILE;