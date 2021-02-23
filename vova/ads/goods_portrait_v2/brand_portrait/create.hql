-- 品牌画像表
DROP TABLE IF EXISTS ads.ads_vova_brand_portrait;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_brand_portrait (
    `second_cat_id` BIGINT COMMENT 'd_二级品类id',
    `brand_id` BIGINT COMMENT 'd_品牌id',
    `clk_cnt_1w` BIGINT COMMENT 'i_近7天点击数',
    `clk_cnt_15d` BIGINT COMMENT 'i_近15天点击数',
    `clk_cnt_1m` BIGINT COMMENT 'i_近30天点击数',
    `collect_cnt_1w` BIGINT COMMENT 'i_近7天收藏数',
    `collect_cnt_15d` BIGINT COMMENT 'i_近15天收藏数',
    `collect_cnt_1m` BIGINT COMMENT 'i_近30天收藏数',
    `add_cat_cnt_1w` BIGINT COMMENT 'i_近7天加购数',
    `add_cat_cnt_15d` BIGINT COMMENT 'i_近15天加购数',
    `add_cat_cnt_1m` BIGINT COMMENT 'i_近30天加购数',
    `sales_vol_1w` BIGINT COMMENT 'i_近7天销量',
    `sales_vol_15d` BIGINT COMMENT 'i_近15天销量',
    `sales_vol_1m` BIGINT COMMENT 'i_近30天销量',
    `gmv_1w` DECIMAL ( 13, 2 ) COMMENT 'i_近7天gmv',
    `gmv_15d` DECIMAL ( 13, 2 ) COMMENT 'i_近15天gmv',
    `gmv_1m` DECIMAL ( 13, 2 ) COMMENT 'i_近30天gmv',
    `clk_rate_1w` DECIMAL ( 13, 2 ) COMMENT 'i_近7天点击率',
    `clk_rate_15d` DECIMAL ( 13, 2 ) COMMENT 'i_近15天点击率',
    `clk_rate_1m` DECIMAL ( 13, 2 ) COMMENT 'i_近30天点击率',
    `cr_rate_1w` DECIMAL ( 13, 2 ) COMMENT 'i_近7天转换率',
    `cr_rate_15d` DECIMAL ( 13, 2 ) COMMENT 'i_近15天转换率',
    `cr_rate_1m` DECIMAL ( 13, 2 ) COMMENT 'i_近30天转换率'
) COMMENT '品牌画像表' PARTITIONED BY (pt string)  STORED AS PARQUETFILE;
