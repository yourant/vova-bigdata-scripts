-- 站群商品表现表
drop table if exists ads.ads_vova_goods_behave_group_site;
CREATE external TABLE `ads.ads_vova_goods_behave_group_site`(
  `vir_goods_id` bigint COMMENT 'i_商品id',
  `goods_id` bigint COMMENT 'i_虚拟商品id',
  `platform` string COMMENT 'i_终端类型，pc、h5',
  `expre_cnt` int COMMENT 'd_近七日曝光量',
  `clk_cnt` int COMMENT 'd_近七日点击量',
  `order_cnt` int COMMENT 'd_近七日订单量',
  `sales_vol` int COMMENT 'd_近日期销量',
  `expre_uv` int COMMENT 'd_近七日曝光uv',
  `clk_uv` int COMMENT 'd_近七日点击uv',
  `add_cat_uv` int COMMENT 'd_近七日加车uv',
  `order_uv` int COMMENT 'd_近七日下单uv',
  `pay_uv` int COMMENT 'd_近七日支付uv',
  `commodity_id` string COMMENT 'commodity_id',
  `project_name` string COMMENT 'project_name')
COMMENT '站群商品表现表'  PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;

