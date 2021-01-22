CREATE external TABLE dwb.dwb_vova_search_goods_report(
  `event_date` string COMMENT 'd_日期',
  `search_word` string COMMENT 'd_搜索词',
  `search_pv` bigint COMMENT 'i_搜索频次',
  `search_uv` bigint COMMENT 'i_搜索uv',
  `click_uv` bigint COMMENT 'i_商品点击uv',
  `impr_uv` bigint COMMENT 'i_商品曝光uv',
  `click_pv` bigint COMMENT 'i_商品点击数',
  `impr_pv` bigint COMMENT 'i_商品曝光数',
  `cart_uv` bigint COMMENT 'i_加车uv',
  `pay_uv` bigint COMMENT 'i_支付uv',
  `gmv` decimal(10,2) COMMENT '',
  `impr_goods` bigint COMMENT 'i_曝光商品数',
  `datasource` string COMMENT 'd_datasource',
  `platform` string COMMENT 'd_平台',
  `goods_cnt` bigint COMMENT 'i_搜索结果最大商品数',
  `brand_status` string,
  `is_brand` string)
COMMENT '搜索商品统计报表' partitioned by(pt string);