drop table ads.ads_vova_six_rank_mct;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_six_rank_mct(
  mct_id                bigint COMMENT '商家ID',
  mct_name              String COMMENT '商家',
  first_cat_id          bigint COMMENT '一级品类ID',
  add_date              date COMMENT '进入6级店铺日期'
) COMMENT '商家等级表'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table ads.ads_vova_six_rank_mct_arc;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_six_rank_mct_arc(
  mct_id                bigint COMMENT '商家ID',
  mct_name              String COMMENT '商家',
  first_cat_id          bigint COMMENT '一级品类ID',
  is_delete             bigint COMMENT '是否有效'
) COMMENT '六级商家等级表'
  PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';


drop table ads.ads_vova_six_rank_mct_poll_his;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_six_rank_mct_poll_his
(
    mct_id                     bigint COMMENT '商家ID',
    mct_name                   string COMMENT '商家',
    first_cat_id               bigint COMMENT '一级品类ID',
    add_date                   date COMMENT '进入6级店铺日期',
    goods_impression_uv_avg_7d bigint COMMENT '近7天平均曝光uv',
    avg_rate_7d                decimal(16, 4) COMMENT '近7天转化率（支付人数/曝光人数）均值',
    avg_rate_first_cat_7d      decimal(16, 4) COMMENT '5级店铺一级品类近7天转化率均值',
    is_delete                  bigint COMMENT '是否删除'
) COMMENT '6级商家淘汰历史记录表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


/* init
INSERT OVERWRITE TABLE ads.ads_vova_six_rank_mct
SELECT
/*+ REPARTITION(1) */
    m.mct_id,
    m.mct_name,
    g.first_cat_id,
    '2021-04-20' AS add_date
FROM dim.dim_vova_merchant m
         JOIN dim.dim_vova_goods g ON m.mct_id = g.mct_id
WHERE m.mct_name = 'Bakers Store' AND first_cat_id = 194
   OR m.mct_name = 'Home\'s Store' AND first_cat_id = 5713
   OR m.mct_name = 'kak store' AND first_cat_id = 5777
   OR m.mct_name = 'lii' AND first_cat_id = 5712
   OR m.mct_name = 'Maup shop' AND first_cat_id = 5976
   OR m.mct_name = 'SBVCL STORE' AND first_cat_id = 5769
   OR m.mct_name = 'Story of Beauty' AND first_cat_id = 5715
   OR m.mct_name = 'yushu' AND first_cat_id = 5768
   OR m.mct_name = '17UM' AND first_cat_id = 5715
   OR m.mct_name = 'bajianzite' AND first_cat_id = 194
   OR m.mct_name = 'SHEINY' AND first_cat_id = 194
GROUP BY m.mct_id, m.mct_name, g.first_cat_id;
*/
