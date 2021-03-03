drop table ads.ads_vova_search_sort_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_search_sort_d
(
    user_id                   bigint COMMENT '用户ID',
    first_cat_prefer_1w       string COMMENT '近7天一级品类偏好top10',
    second_cat_prefer_1w      string COMMENT '近7天二级品类偏好top10,如果不存在二级品类则取一级',
    second_cat_max_click_1m   bigint COMMENT '近一个月点击最多二级品类，如果不存在二级品类则取一级',
    second_cat_max_collect_1m bigint COMMENT '近一个月收藏最多二级品类，如果不存在二级品类则取一级',
    second_cat_max_cart_1m    bigint COMMENT '近一个月加购最多二级品类，如果不存在二级品类则取一级',
    second_cat_max_order_1m   bigint COMMENT '近一个月下单最多二级品类，如果不存在二级品类则取一级',
    brand_prefer_1w           string COMMENT '近7天品牌偏好top10',
    brand_prefer_his          string COMMENT '历史品牌偏好top10',
    brand_max_click_1m        bigint COMMENT '近30天点击最多品牌',
    brand_max_collect_1m      bigint COMMENT '近30天收藏最多品牌',
    brand_max_cart_1m         bigint COMMENT '近30天加购最多品牌',
    brand_max_order_1m        bigint COMMENT '近30天下单最多品牌',
    price_prefer_1w           string COMMENT '近7天价格偏好层级'
) COMMENT '搜索排序'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE TABLE ads_search_sort_d (
  user_id int(11)  NOT NULL COMMENT '用户ID',
  first_cat_prefer_1w varchar(128) COMMENT '近7天一级品类偏好top10',
  second_cat_prefer_1w varchar(128) COMMENT '近7天二级品类偏好top10,如果不存在二级品类则取一级',
  second_cat_max_click_1m int(10)  COMMENT '近一个月点击最多二级品类，如果不存在二级品类则取一级',
  second_cat_max_collect_1m int(20) COMMENT '近一个月收藏最多二级品类，如果不存在二级品类则取一级',
  second_cat_max_cart_1m int(20)  COMMENT '近一个月加购最多二级品类，如果不存在二级品类则取一级',
  second_cat_max_order_1m int(20)  COMMENT '近一个月下单最多二级品类，如果不存在二级品类则取一级',
  brand_prefer_1w varchar(128)  COMMENT '近7天品牌偏好top10',
  brand_prefer_his varchar(128)  COMMENT '历史品牌偏好top10',
  brand_max_click_1m int(10)  COMMENT '近30天点击最多品牌',
  brand_max_collect_1m int(10)  COMMENT '近30天收藏最多品牌',
  brand_max_cart_1m int(10)  COMMENT '近30天加购最多品牌',
  brand_max_order_1m int(10)  COMMENT '近30天下单最多品牌',
  price_prefer_1w varchar(10)  COMMENT '近7天价格偏好层级',
  PRIMARY KEY (user_id),
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='搜索排序';

