-- 用户画像品牌偏好统计表
CREATE external TABLE `ads.ads_vova_buyer_portrait_brand_likes`(
  `buyer_id` bigint COMMENT 'd_买家id',
  `brand_id` bigint COMMENT 'd_品牌id',
  `expre_cnt_1w` bigint COMMENT 'i_近7天品牌曝光次数',
  `expre_cnt_15d` bigint COMMENT 'i_近15天品牌曝光次数',
  `expre_cnt_1m` bigint COMMENT 'i_近30天品牌曝光次数',
  `clk_cnt_1w` bigint COMMENT 'i_近7天品牌点击次数',
  `clk_cnt_15d` bigint COMMENT 'i_近15天品牌点击次数',
  `clk_cnt_1m` bigint COMMENT 'i_近30天品牌点击次数',
  `clk_valid_cnt_1w` bigint COMMENT 'i_近7天品牌有效点击次数',
  `clk_valid_cnt_15d` bigint COMMENT 'i_近15天品牌有效点击次数',
  `clk_valid_cnt_1m` bigint COMMENT 'i_近30天品牌有效点击次数',
  `collect_cnt_1w` bigint COMMENT 'i_近7天品牌收藏次数',
  `collect_cnt_15d` bigint COMMENT 'i_近15天品牌收藏次数',
  `collect_cnt_1m` bigint COMMENT 'i_近30天品牌收藏次数',
  `add_cat_cnt_1w` bigint COMMENT 'i_近7天品牌加购次数',
  `add_cat_cnt_15d` bigint COMMENT 'i_近15天品牌加购次数',
  `add_cat_cnt_1m` bigint COMMENT 'i_近30天品牌加购次数',
  `ord_cnt_1w` bigint COMMENT 'i_近7天品牌购买次数',
  `ord_cnt_15d` bigint COMMENT 'i_近15天品牌购买次数',
  `ord_cnt_1m` bigint COMMENT 'i_近30天品牌购买次数',
  `min_clk_day_gap` string COMMENT '用户最后一次点击该品牌到现在的时间',
  `gmv_1w` decimal(14,4) COMMENT 'i_近7天品牌购买gmv',
  `gmv_15d` decimal(14,4) COMMENT 'i_近15天品牌购买gmv',
  `gmv_1m` decimal(14,4) COMMENT 'i_近30天品牌购买gmv')
COMMENT '用户画像品牌偏好统计表'
PARTITIONED BY ( `pt` string) stored as parquetfile;


CREATE external TABLE `ads.ads_vova_buyer_portrait_brand_likes_with_click_15d`(
  `buyer_id` bigint COMMENT 'd_买家id',
  `brand_id` bigint COMMENT 'd_品牌id',
  `expre_cnt_1w` bigint COMMENT 'i_近7天品牌曝光次数',
  `expre_cnt_15d` bigint COMMENT 'i_近15天品牌曝光次数',
  `expre_cnt_1m` bigint COMMENT 'i_近30天品牌曝光次数',
  `clk_cnt_1w` bigint COMMENT 'i_近7天品牌点击次数',
  `clk_cnt_15d` bigint COMMENT 'i_近15天品牌点击次数',
  `clk_cnt_1m` bigint COMMENT 'i_近30天品牌点击次数',
  `clk_valid_cnt_1w` bigint COMMENT 'i_近7天品牌有效点击次数',
  `clk_valid_cnt_15d` bigint COMMENT 'i_近15天品牌有效点击次数',
  `clk_valid_cnt_1m` bigint COMMENT 'i_近30天品牌有效点击次数',
  `collect_cnt_1w` bigint COMMENT 'i_近7天品牌收藏次数',
  `collect_cnt_15d` bigint COMMENT 'i_近15天品牌收藏次数',
  `collect_cnt_1m` bigint COMMENT 'i_近30天品牌收藏次数',
  `add_cat_cnt_1w` bigint COMMENT 'i_近7天品牌加购次数',
  `add_cat_cnt_15d` bigint COMMENT 'i_近15天品牌加购次数',
  `add_cat_cnt_1m` bigint COMMENT 'i_近30天品牌加购次数',
  `ord_cnt_1w` bigint COMMENT 'i_近7天品牌购买次数',
  `ord_cnt_15d` bigint COMMENT 'i_近15天品牌购买次数',
  `ord_cnt_1m` bigint COMMENT 'i_近30天品牌购买次数')
COMMENT '用户画像品牌偏好统计表(近15日有点击)' stored as parquetfile;


CREATE external TABLE `ads.ads_vova_buyer_portrait_brand_likes_medium_term`(
  `buyer_id` bigint COMMENT 'd_买家id',
  `brand_id` bigint COMMENT 'd_品牌id',
  `expre_cnt` int COMMENT 'i_曝光次数',
  `clk_cnt` int COMMENT 'i_点击次数',
  `clk_valid_cnt` int COMMENT 'i_有效点击次数',
  `collect_cnt` int COMMENT 'i_收藏次数',
  `add_cat_cnt` int COMMENT 'i_加购次数',
  `ord_cnt` int COMMENT 'i_购买次数',
  `gmv` decimal(14,4) COMMENT 'i_购买gmv',
  `min_clk_day_gap` string COMMENT 'i_用户最后一次点击该品牌到现在的时间')
COMMENT '用户画像品牌中期偏好统计表(近16-60天)'
PARTITIONED BY ( `pt` string) stored as parquetfile;


CREATE external TABLE `ads.ads_vova_buyer_portrait_brand_likes_long_term`(
  `buyer_id` bigint COMMENT 'd_买家id',
  `brand_id` bigint COMMENT 'd_品牌id',
  `ord_cnt` int COMMENT 'i_购买次数',
  `gmv` decimal(14,4) COMMENT 'i_购买gmv')
COMMENT '用户画像品牌长期偏好统计表(61天之前)'
PARTITIONED BY ( `pt` string) stored as parquetfile;


CREATE external TABLE `ads.ads_vova_buyer_portrait_brand_likes_exp`(
  `buyer_id` bigint COMMENT 'd_买家id',
  `brand_id` bigint COMMENT 'd_品牌id',
  `likes_weight` decimal(13,3) COMMENT 'i_品牌偏好度',
  `likes_weight_short` decimal(13,3) COMMENT 'i_品牌短期偏好度',
  `likes_weight_medium` decimal(13,3) COMMENT 'i_品牌中期偏好度',
  `likes_weight_long` decimal(13,3) COMMENT 'i_品牌长期偏好度',
  `likes_weight_synth` decimal(13,3) COMMENT 'i_品牌综合偏好度')
COMMENT '用户画像品牌偏好统计表品牌偏好度表(自然数计算)'
PARTITIONED BY ( `pt` string) stored as parquetfile;