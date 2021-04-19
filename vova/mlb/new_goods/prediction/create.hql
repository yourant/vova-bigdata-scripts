DROP TABLE mlb.mlb_vova_hot_goods_prediction_base;
create external TABLE mlb.mlb_vova_hot_goods_prediction_base
(
cat_id          bigint COMMENT '商品类目id',
good_id         bigint COMMENT '商品id',
first_on_sale_date string COMMENT '商品首次上架日期',
on_sale_date    string COMMENT '商品上架日期',
test_start_date string COMMENT '测款开始日期',
test_end_date   string COMMENT '测款结束日期',
is_test_scuess  bigint COMMENT '是否测款成功',
is_hot          bigint COMMENT '是否爆款'
) COMMENT '爆款标签数据' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION "s3://vova-mlb/REC/data/hot_prediction/hot_goods_prediction_base"
STORED AS textfile;

MSCK REPAIR TABLE mlb.mlb_vova_hot_goods_prediction_base;

DROP TABLE mlb.mlb_vova_hot_goods_prediction_pre;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_hot_goods_prediction_pre
(
    goods_id           bigint,
    impressions        bigint,
    test_start_date    string,
    test_end_date      string,
    is_test_scuess     bigint,
    test_type          bigint,
    test_goods_result_status          bigint
) COMMENT 'mlb_vova_hot_goods_prediction_pre' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

DROP TABLE mlb.mlb_vova_hot_goods_prediction_model_feature_data;
CREATE EXTERNAL TABLE mlb.mlb_vova_hot_goods_prediction_model_feature_data
(
    goods_id              string,
    cat_id               string,
    first_on_sale_date   string,
    on_sale_date         string,
    test_start_date      string,
    test_end_date        string,
    is_test_scuess       string,
    obs_date             string,
    first_cat_id         string,
    second_cat_id        string,
    mct_id               string,
    shop_price           string,
    gs_discount          string,
    shipping_fee         string,
    comment_cnt_6m       string,
    comment_good_cnt_6m  string,
    comment_bad_cnt_6m   string,
    gmv_1w               string,
    gmv_15d              string,
    gmv_1m               string,
    sales_vol_1w         string,
    sales_vol_15d        string,
    sales_vol_1m         string,
    expre_cnt_1w         string,
    expre_cnt_15d        string,
    expre_cnt_1m         string,
    clk_cnt_1w           string,
    clk_cnt_15d          string,
    clk_cnt_1m           string,
    collect_cnt_1w       string,
    collect_cnt_15d      string,
    collect_cnt_1m       string,
    add_cat_cnt_1w       string,
    add_cat_cnt_15d      string,
    add_cat_cnt_1m       string,
    clk_rate_1w          string,
    clk_rate_15d         string,
    clk_rate_1m          string,
    pay_rate_1w          string,
    pay_rate_15d         string,
    pay_rate_1m          string,
    add_cat_rate_1w      string,
    add_cat_rate_15d     string,
    add_cat_rate_1m      string,
    cr_rate_1w           string,
    cr_rate_15d          string,
    cr_rate_1m           string,
    gs_gender            string,
    mp_clk_pv_1w         string,
    mp_clk_pv_15d        string,
    mp_clk_pv_1m         string,
    mp_cart_pv_1w        string,
    mp_cart_pv_15d       string,
    mp_cart_pv_1m        string,
    mp_clk_pv_1w_rk      string,
    mp_clk_pv_15d_rk     string,
    mp_clk_pv_1m_rk      string,
    mp_cart_pv_1w_rk     string,
    mp_cart_pv_15d_rk    string,
    mp_cart_pv_1m_rk     string,
    is_new               string,
    reg_to_now_days      string,
    average_price        string,
    cur_gmv              string,
    cur_uv               string,
    cur_pv               string,
    cur_payed_uv         string,
    cur_ctr              string,
    bs_cur_ctr           string,
    cur_cr               string,
    bs_cur_cr            string,
    mct_gmv_1m           string,
    atv_2m               string,
    inter_rate_3_6w      string,
    bs_inter_rate_3_6w   string,
    lrf_rate_9_12w       string,
    bs_lrf_rate_9_12w    string,
    nlrf_rate_5_8w       string,
    bs_nlrf_rate_5_8w    string,
    rep_rate_1mth        string,
    bs_rep_rate_1mth     string,
    cohort_rate_1mth     string,
    bs_cohort_rate_1mth  string,
    rf_rate_1_3m         string,
    bs_rf_rate_1_3m      string,
    proper_rate_5_8w     string,
    bs_proper_rate_5_8w  string,
    uv_1m                string,
    payed_uv_1m          string,
    bs_avg_cr_1m         string,
    sell_goods_cnt_1m    string,
    on_sale_goods_cnt_1m string,
    is_hot               string
) COMMENT 'mlb_vova_hot_goods_prediction_model_feature_data' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

DROP TABLE mlb.mlb_vova_hot_goods_prediction_data;
CREATE EXTERNAL TABLE mlb.mlb_vova_hot_goods_prediction_data
(
    goods_id            string,
    cat_id               string,
    shop_price          string,
    gs_discount         string,
    shipping_fee        string,
    comment_cnt_6m      string,
    comment_good_cnt_6m string,
    comment_bad_cnt_6m  string,
    gmv_1w              string,
    gmv_15d             string,
    gmv_1m              string,
    sales_vol_1w        string,
    sales_vol_15d       string,
    sales_vol_1m        string,
    expre_cnt_1w        string,
    expre_cnt_15d       string,
    expre_cnt_1m        string,
    clk_cnt_1w          string,
    clk_cnt_15d         string,
    clk_cnt_1m          string,
    collect_cnt_1w      string,
    collect_cnt_15d     string,
    collect_cnt_1m      string,
    add_cat_cnt_1w      string,
    add_cat_cnt_15d     string,
    add_cat_cnt_1m      string,
    clk_rate_1w         string,
    clk_rate_15d        string,
    clk_rate_1m         string,
    pay_rate_1w         string,
    pay_rate_15d        string,
    pay_rate_1m         string,
    add_cat_rate_1w     string,
    add_cat_rate_15d    string,
    add_cat_rate_1m     string,
    cr_rate_1w          string,
    cr_rate_15d         string,
    cr_rate_1m          string,
    gs_gender           string,
    mp_clk_pv_1w        string,
    mp_clk_pv_15d       string,
    mp_clk_pv_1m        string,
    mp_cart_pv_1w       string,
    mp_cart_pv_15d      string,
    mp_cart_pv_1m       string,
    mp_clk_pv_1w_rk     string,
    mp_clk_pv_15d_rk    string,
    mp_clk_pv_1m_rk     string,
    mp_cart_pv_1w_rk    string,
    mp_cart_pv_15d_rk   string,
    mp_cart_pv_1m_rk    string,
    is_new              string,
    reg_to_now_days     string,
    average_price       string,
    cur_gmv             string,
    cur_uv              string,
    cur_pv              string,
    cur_payed_uv        string,
    cur_ctr             string,
    bs_cur_ctr          string,
    cur_cr              string,
    bs_cur_cr           string,
    mct_gmv_1m          string,
    atv_2m              string,
    inter_rate_3_6w     string,
    bs_inter_rate_3_6w  string,
    lrf_rate_9_12w      string,
    bs_lrf_rate_9_12w   string,
    nlrf_rate_5_8w      string,
    bs_nlrf_rate_5_8w   string,
    rep_rate_1mth       string,
    bs_rep_rate_1mth    string,
    cohort_rate_1mth    string,
    bs_cohort_rate_1mth string,
    rf_rate_1_3m        string,
    bs_rf_rate_1_3m     string,
    proper_rate_5_8w    string,
    bs_proper_rate_5_8w string,
    uv_1m               string,
    payed_uv_1m         string,
    bs_avg_cr_1m        string,
    sell_goods_cnt_1m   string,
    on_sale_goods_cnt_1m string
) COMMENT 'mlb_vova_hot_goods_prediction_data' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE;

DROP TABLE mlb.mlb_vova_hot_prediction_result_data;
create external table mlb.mlb_vova_hot_prediction_result_data
(
    goods_id         STRING,
    prob_score       STRING
) COMMENT '爆款预测结果数据' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
    LOCATION "s3://vova-mlb/REC/data/hot_prediction/result_data"
;


DROP TABLE mlb.mlb_vova_new_goods_predicte_result;
CREATE TABLE IF NOT EXISTS mlb.mlb_vova_new_goods_predicte_result
(
    goods_id           bigint,
    cat_id             bigint,
    predicte_score     decimal(18, 10),
    rank               bigint
) COMMENT 'mlb_vova_new_goods_predicte_result' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

