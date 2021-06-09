[9617]首页策略调整&用户rating分数调整并支持ab实验

首页兜底当前是不做打散的，需要对首页的兜底的产品做一定的打散，以及对兜底的商品做一定的改进，增加首页的转化率和用户体验。具体的逻辑如下图：

经过性别和用户阶段两个字段做了两级的用户商品兜底策略

表字段如下：

male    1   0.7
male    2   0.6
female  1   0.6
unknown 2   0.9
result

create TABLE mlb.mlb_vova_gender_hot_goods_d
(
    gender               int              COMMENT '性别:1:男；0:女；-1:通用',
    goods_id             bigint           COMMENT '商品id',
    goods_score          double           COMMENT '商品综合评分'
) COMMENT '性别热门兜底' PARTITIONED BY (pt STRING)
STORED AS parquet
;

其中男/女 商品评分top50 ,是指指定的second_cat_id下的商品；

男性商品取自 second_cat_id:
5784,5789,5773,5793,5721,5795,5786,5785,5841,5830,5839,5732,5834,6375,5832,5733,5794,5836,5831,
5835,5787,5833,5792,5891,5731,5737,5790,5975,5890,5974,5966,5838,5736,5892,5897,5837,5894,5893

女性 second_cat_id：
5741,6374, 165,5781,5903,5902,5799,5905, 195,5796,5939,5904,5962,5954,3001,5963,5909,5907,5797,5800,5810,
 164,3004,5798, 171,5811,5944, 166,5928,5807,5825, 173,5812,5927,5930,5960,5814,6008,6009,5813,5929,6007,
5906,5967,5932,5808,5788,5805,5815,5961,6006,5911,5988,5816,5934,5803,5931,5933,5935

通用商品second_cat_id:
5940,5964,5823,5821,5883,5994,5991,5711,5990,5992,5993,5978,5775,5782,5717,5721,5718,5803,5941,5735,5804

取各自限制的品类的商品的top50商品，按照 mlb.mlb_vova_rec_b_catgoods_score_d 中的商品综合评分 overall_cat_score 来从高到底排序
其中商品需要在 ads.ads_vova_goods_portrait 表中 is_recommend = 1

[9617]
表结构：性别，goods_id，商品综合评分
过滤条件：取该性别下列出的所有品类，每个品类取TOP50商品（不够50个全部取出）

依赖: ads.ads_vova_goods_portrait ; mlb.mlb_vova_rec_b_catgoods_score_d

create TABLE mlb.mlb_vova_gender_hot_goods_d
(
    gender               int              COMMENT '性别:1:男；0:女；-1:通用',
    goods_id             bigint           COMMENT '商品id',
    goods_score          double           COMMENT '商品综合评分'
) COMMENT '性别热门兜底' PARTITIONED BY (pt STRING)
STORED AS parquet
;
