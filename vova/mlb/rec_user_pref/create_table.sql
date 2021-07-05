-- 基础表：dws.dws_vova_buyer_goods_behave
CREATE table tmp.tmp_vova_buyer_cat_rating_d
(
   buyer_id         BIGINT COMMENT '用户ID',
   cat_id           BIGINT COMMENT '子品类ID',
   cat_type         STRING COMMENT '品类类型',
   cur_expre_cnt    BIGINT COMMENT '当天曝光商品次数',
   cur_clk_cnt      BIGINT COMMENT '当天点击商品次数',
   cur_collect_cnt  BIGINT COMMENT '当天收藏商品次数',
   cur_add_cart_cnt BIGINT COMMENT '当天加购商品次数',
   cur_ord_cnt      BIGINT COMMENT '当天购买商品次数',
   cur_rating       DOUBLE COMMENT '当天用户偏好分',
   his_expre_cnt    BIGINT COMMENT '历史曝光次数',
   his_clk_cnt      BIGINT COMMENT '历史点击次数',
   his_collect_cnt  BIGINT COMMENT '历史收藏次数',
   his_add_cart_cnt BIGINT COMMENT '历史加购次数',
   his_ord_cnt      BIGINT COMMENT '历史购买次数',
   his_rating       DOUBLE COMMENT '历史用户偏好分',
   his_expre_score  DOUBLE COMMENT '历史曝光负反馈分数',
   pref             DOUBLE COMMENT '偏好'
)
PARTITIONED BY(pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet;
-- 构建偏好，一级品类、二级品类、子品类
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_buyer_cat_rating_d
(
   buyer_id         BIGINT COMMENT '用户ID',
   cat_id           BIGINT COMMENT '子品类ID',
   cat_type         STRING COMMENT '品类类型',
   cur_expre_cnt    BIGINT COMMENT '当天曝光商品次数',
   cur_clk_cnt      BIGINT COMMENT '当天点击商品次数',
   cur_collect_cnt  BIGINT COMMENT '当天收藏商品次数',
   cur_add_cart_cnt BIGINT COMMENT '当天加购商品次数',
   cur_ord_cnt      BIGINT COMMENT '当天购买商品次数',
   cur_rating       DOUBLE COMMENT '当天用户偏好分',
   his_expre_cnt    BIGINT COMMENT '历史曝光次数',
   his_clk_cnt      BIGINT COMMENT '历史点击次数',
   his_collect_cnt  BIGINT COMMENT '历史收藏次数',
   his_add_cart_cnt BIGINT COMMENT '历史加购次数',
   his_ord_cnt      BIGINT COMMENT '历史购买次数',
   his_rating       DOUBLE COMMENT '历史用户偏好分',
   his_expre_score  DOUBLE COMMENT '历史曝光负反馈分数',
   pref             DOUBLE COMMENT '偏好'
)
PARTITIONED BY(pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/base/mlb_vova_buyer_cat_rating_d"
;

--DROP TABLE IF EXISTS mlb.mlb_vova_buyer_goods_rating_d;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_buyer_goods_rating_d
(
   buyer_id      BIGINT  COMMENT '用户ID',
   goods_id      BIGINT  COMMENT '商品ID',
   first_cat_id  BIGINT  COMMENT '一级品类',
   second_cat_id BIGINT  COMMENT '二级品类',
   cat_id        BIGINT  COMMENT '子品类',
   cur_rating    DOUBLE  COMMENT '当天评分',
   his_rating    DOUBLE  COMMENT '历史评分'
) COMMENT '用户商品偏好表'
PARTITIONED BY(pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/base/mlb_vova_buyer_goods_rating_d"
;

DROP TABLE IF EXISTS mlb.mlb_vova_buyer_cat_rating_offline;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_buyer_cat_rating_offline
(
   buyer_id         BIGINT COMMENT '用户ID',
   cat_id           BIGINT COMMENT '子品类ID',
   cat_type         STRING COMMENT '品类类型',
   his_rating       DOUBLE COMMENT '历史用户偏好分',
   his_expre_socre  DOUBLE COMMENT '历史曝光负反馈分数',
   pref             DOUBLE COMMENT '偏好'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/base/mlb_vova_buyer_cat_rating_offline"
;

DROP TABLE IF EXISTS mlb.mlb_vova_buyer_goods_rating_offline;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_buyer_goods_rating_offline
(
   buyer_id      BIGINT  COMMENT '用户ID',
   goods_id      BIGINT  COMMENT '商品ID',
   first_cat_id  BIGINT  COMMENT '一级品类',
   second_cat_id BIGINT  COMMENT '二级品类',
   cat_id        BIGINT  COMMENT '子品类',
   his_rating    DOUBLE  COMMENT '历史评分'
) COMMENT '用户商品偏好表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
LOCATION "s3://vova-mlb/REC/data/base/mlb_vova_buyer_goods_rating_offline"
;