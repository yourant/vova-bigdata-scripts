DROP TABLE mlb.mlb_vova_hot_goods_group;
create external TABLE mlb.mlb_vova_hot_goods_group
(
    group_number     string COMMENT 'group_number',
    goods_id         bigint COMMENT '商品id',
    gmv_cr           decimal(14,4) COMMENT 'gmv_cr'
) COMMENT '热销商品组表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/hot_goods_group";

DROP TABLE mlb.mlb_vova_hot_goods_group_vec;
create external TABLE mlb.mlb_vova_hot_goods_group_vec
(
    goods_id         bigint COMMENT '商品id',
    img_vec          string COMMENT 'img_vec'
) COMMENT '热销商品组表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/hot_goods_img_vec";

DROP TABLE mlb.mlb_vova_new_goods_group_vec;
create external TABLE mlb.mlb_vova_new_goods_group_vec
(
    goods_id         bigint COMMENT '商品id',
    img_vec          string COMMENT 'img_vec'
) COMMENT '热销商品组表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/new_goods_img_vec";


DROP TABLE mlb.mlb_vova_new_goods_group;
create external TABLE mlb.mlb_vova_new_goods_group
(
    group_number     string COMMENT 'group_number',
    goods_id         bigint COMMENT '商品id',
    cat_id           bigint COMMENT 'cat_id',
    brand_id         bigint COMMENT 'brand_id'
) COMMENT '热销商品组表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/new_goods_group";


DROP TABLE mlb.mlb_vova_new_goods_base;
create external TABLE mlb.mlb_vova_new_goods_base
(
    goods_id             bigint COMMENT '商品id',
    brand_id             bigint COMMENT 'brand_id',
    goods_name           string COMMENT '',
    shop_price           decimal(14,4) COMMENT '',
    goods_weight         decimal(14,4) COMMENT '',
    first_cat_id         bigint COMMENT '',
    second_cat_id        bigint COMMENT '',
    cat_id               bigint COMMENT '',
    keywords             string COMMENT '',
    mct_id               bigint COMMENT '',
    reg_time             timestamp COMMENT '',
    mct_cat_desc         string COMMENT '',
    sale_region_desc     string COMMENT '',
    logistics_type_desc  string COMMENT '',
    is_banned            string COMMENT '',
    mct_status           string COMMENT '',
    first_customer_buy_time timestamp COMMENT '商家首次出单时间',
    mct_rank             bigint COMMENT '',
    bs_avg_cr_1m         double COMMENT '近30天平均转化率',
    bs_cohort_rate_1mth  double COMMENT '月留存购率',
    bs_inter_rate_3_6w   double COMMENT '前42天到前21天的订单7天上网率',
    bs_rf_rate_1_3m      double COMMENT '前90天至前30天订单的退款率',
    mct_pop_prob         decimal(14,4) COMMENT '所属商铺出爆款的概率',
    second_diff_mean_price  decimal(14,4) COMMENT '与当前second_cat 近一月内销量最好商品的平均价格价格 的差值',
    second_diff_mid_price   decimal(14,4) COMMENT '与当前second_cat 近一月内销量最好商品的中位数价格 的差值',
    is_mct_pop_second_cat   bigint COMMENT '与当同商铺下近一月内销量最好商品的second_cat_id 是否相同',
    is_mct_pop_fisrt_cat    bigint COMMENT '与当同商铺下近一月内销量最好商品的first_cat_id 是否相同',
    diff_atv_2m          decimal(14,4) COMMENT '与当前店铺的客单价之间的差值',
    avg_goods_payed      decimal(14,4) COMMENT '当前店铺平均商品支付率',
    ord_cnt              bigint COMMENT '商品被购买次数',
    add_timestamp        timestamp COMMENT '商品上架时间',
    goods_tag            bigint COMMENT '商品近一个月曝光超5000/被购买',
    expre_cnt_1m         bigint COMMENT '近1月曝光'
) COMMENT '热销商品组表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/new_goods_base/new_goods_base";

DROP TABLE mlb.mlb_vova_new_goods_predict;
create external TABLE mlb.mlb_vova_new_goods_predict
(
    goods_id             bigint COMMENT '商品id',
    brand_id             bigint COMMENT 'brand_id',
    goods_name           string COMMENT '',
    shop_price           decimal(14,4) COMMENT '',
    goods_weight         decimal(14,4) COMMENT '',
    first_cat_id         bigint COMMENT '',
    second_cat_id        bigint COMMENT '',
    cat_id               bigint COMMENT '',
    keywords             string COMMENT '',
    mct_id               bigint COMMENT '',
    reg_time             timestamp COMMENT '',
    mct_cat_desc         string COMMENT '',
    sale_region_desc     string COMMENT '',
    logistics_type_desc  string COMMENT '',
    is_banned            string COMMENT '',
    mct_status           string COMMENT '',
    first_customer_buy_time timestamp COMMENT '商家首次出单时间',
    mct_rank             bigint COMMENT '',
    bs_avg_cr_1m         double COMMENT '近30天平均转化率',
    bs_cohort_rate_1mth  double COMMENT '月留存购率',
    bs_inter_rate_3_6w   double COMMENT '前42天到前21天的订单7天上网率',
    bs_rf_rate_1_3m      double COMMENT '前90天至前30天订单的退款率',
    mct_pop_prob         decimal(14,4) COMMENT '所属商铺出爆款的概率',
    second_diff_mean_price  decimal(14,4) COMMENT '与当前second_cat 近一月内销量最好商品的平均价格价格 的差值',
    second_diff_mid_price   decimal(14,4) COMMENT '与当前second_cat 近一月内销量最好商品的中位数价格 的差值',
    is_mct_pop_second_cat   bigint COMMENT '与当同商铺下近一月内销量最好商品的second_cat_id 是否相同',
    is_mct_pop_fisrt_cat    bigint COMMENT '与当同商铺下近一月内销量最好商品的first_cat_id 是否相同',
    diff_atv_2m          decimal(14,4) COMMENT '与当前店铺的客单价之间的差值',
    avg_goods_payed      decimal(14,4) COMMENT '当前店铺平均商品支付率',
    ord_cnt              bigint COMMENT '商品被购买次数',
    add_timestamp            timestamp COMMENT '商品上架时间',
    expre_cnt_1m         bigint COMMENT '近1月曝光'
) COMMENT '热销商品组表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/new_goods_base/new_goods_predict";

DROP TABLE mlb.mlb_vova_new_goods_group_key_words_base;
create external TABLE mlb.mlb_vova_new_goods_group_key_words_base
(
    goods_id         bigint COMMENT '商品id',
    cat_id           bigint COMMENT 'cat_id',
    brand_id         bigint COMMENT 'brand_id',
    goods_name       string COMMENT 'goods_name'
) COMMENT '热销商品表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/new_goods";

DROP TABLE mlb.mlb_vova_new_goods_group_key_words;
create external TABLE mlb.mlb_vova_new_goods_group_key_words
(
    key_words        string COMMENT '热搜词',
    search_counts    bigint COMMENT '搜索次数'
) COMMENT '关键词表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    STORED AS PARQUET
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/key_words";


DROP TABLE mlb.mlb_vova_rec_img_relate_group_nb_d;
create external TABLE mlb.mlb_vova_rec_img_relate_group_nb_d
(
    group_number    string COMMENT '分组号',
    goods_id        bigint COMMENT 'goods_id',
    cat_id          bigint COMMENT 'cat_id',
    score           DECIMAL(14,4) COMMENT '得分'
) COMMENT '关键词表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS textfile
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/predict_score_result/no_brand_img_relate_group"
    ;


msck repair table mlb.mlb_vova_rec_new_goods_base_nb_d;
select count(*),count(DISTINCT goods_id),pt from mlb.mlb_vova_rec_new_goods_base_nb_d group by pt;
DROP TABLE mlb.mlb_vova_rec_new_goods_base_nb_d;
create external TABLE mlb.mlb_vova_rec_new_goods_base_nb_d
(
    goods_id        bigint COMMENT 'goods_id',
    cat_id          bigint COMMENT 'cat_id',
    score           DECIMAL(14,4) COMMENT '得分'
) COMMENT '关键词表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS textfile
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/predict_score_result/base_goods_info"
    ;


msck repair table mlb.mlb_vova_rec_new_goods_text_nb_d;
DROP TABLE mlb.mlb_vova_rec_new_goods_text_nb_d;
create external TABLE mlb.mlb_vova_rec_new_goods_text_nb_d
(
    goods_id        bigint COMMENT 'goods_id',
    cat_id          bigint COMMENT 'cat_id',
    score           DECIMAL(14,4) COMMENT '得分'
) COMMENT '关键词表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS textfile
    LOCATION "s3://vova-mlb/REC/data/new_goods_rec/predict_score_result/text_goods_info"
    ;

