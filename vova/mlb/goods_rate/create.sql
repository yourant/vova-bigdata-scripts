DROP TABLE IF EXISTS mlb.mlb_vova_goods_rate;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_goods_rate
(
    goods_id                 int,
    onsale_days              int,
    first_cat_id             int,
    second_cat_id            int,
    third_cat_id             int,
    forth_cat_id             int,
    cat_id                   int,
    shop_price               double,
    is_brand                 int,
    gather_rate              double,
    inter_rate_3_6w          double,
    nlrf_rate_5_8w           double,
    lrf_rate_9_12w           double,
    mct_score                double,
    mct_level                double,
    comment_cnt_6m           int,
    good_comment_cnt_6m      int,
    bad_comment_cnt_6m       int,
    good_comment_rate_6m     double,
    bad_comment_rate_6m      double,
    expre_cnt_1d             int,
    expre_uv_1d              int,
    clk_cnt_1d               int,
    clk_uv_1d                int,
    clk_rate_1d              double,
    cr_rate_1d               double,
    expre_cnt_per_u_1d       double,
    clk_cnt_per_u_1d         double,
    gmv_1d                   double,
    collect_cnt_1d           int,
    collect_uv_1d            int,
    add_cart_cnt_1d          int,
    add_cart_uv_1d           int,
    order_cnt_1d             int,
    order_uv_1d              int,
    sale_vol_1d              int,
    comment_cnt_1d           int,
    good_comment_cnt_1d      int,
    bad_comment_cnt_1d       int,
    avg_stay_time_1d         double,
    refund_order_cnt_1d      int,
    refund_amt_1d            double,
    expre_on_second_rate_1d  double,
    clk_on_second_rate_1d    double,
    gmv_on_second_rate_1d    double,
    expre_on_brand_rate_1d   double,
    clk_on_brand_rate_1d     double,
    gmv_on_brand_rate_1d     double,
    expre_effciency_1d       double,
    gr_1d                    double,
    add_cart_on_mct_rate_1d  double,
    collect_on_mct_rate_1d   double,
    expre_on_mct_rate_1d     double,
    clk_on_mct_rate_1d       double,
    gmv_on_mct_rate_1d       double,
    expre_cnt_3d             int,
    expre_uv_3d              int,
    clk_cnt_3d               int,
    clk_uv_3d                int,
    clk_rate_3d              double,
    cr_rate_3d               double,
    expre_cnt_per_u_3d       double,
    clk_cnt_per_u_3d         double,
    gmv_3d                   double,
    collect_cnt_3d           int,
    collect_uv_3d            int,
    add_cart_cnt_3d          int,
    add_cart_uv_3d           int,
    order_cnt_3d             int,
    order_uv_3d              int,
    sale_vol_3d              int,
    comment_cnt_3d           int,
    good_comment_cnt_3d      int,
    bad_comment_cnt_3d       int,
    avg_stay_time_3d         double,
    refund_order_cnt_3d      int,
    refund_amt_3d            double,
    expre_on_second_rate_3d  double,
    clk_on_second_rate_3d    double,
    gmv_on_second_rate_3d    double,
    expre_on_brand_rate_3d   double,
    clk_on_brand_rate_3d     double,
    gmv_on_brand_rate_3d     double,
    expre_effciency_3d       double,
    gr_3d                    double,
    add_cart_on_mct_rate_3d  double,
    collect_on_mct_rate_3d   double,
    expre_on_mct_rate_3d     double,
    clk_on_mct_rate_3d       double,
    gmv_on_mct_rate_3d       double,
    expre_cnt_7d             int,
    expre_uv_7d              int,
    clk_cnt_7d               int,
    clk_uv_7d                double,
    clk_rate_7d              double,
    cr_rate_7d               int,
    expre_cnt_per_u_7d       double,
    clk_cnt_per_u_7d         double,
    gmv_7d                   double,
    collect_cnt_7d           int,
    collect_uv_7d            int,
    add_cart_cnt_7d          int,
    add_cart_uv_7d           int,
    order_cnt_7d             int,
    order_uv_7d              int,
    sale_vol_7d              int,
    comment_cnt_7d           int,
    good_comment_cnt_7d      int,
    bad_comment_cnt_7d       int,
    avg_stay_time_7d         double,
    refund_order_cnt_7d      int,
    refund_amt_7d            double,
    expre_on_second_rate_7d  double,
    clk_on_second_rate_7d    double,
    gmv_on_second_rate_7d    double,
    expre_on_brand_rate_7d   double,
    clk_on_brand_rate_7d     double,
    gmv_on_brand_rate_7d     double,
    expre_effciency_7d       double,
    gr_7d                    double,
    add_cart_on_mct_rate_7d  double,
    collect_on_mct_rate_7d   double,
    expre_on_mct_rate_7d     double,
    clk_on_mct_rate_7d       double,
    gmv_on_mct_rate_7d       double,
    expre_cnt_14d            int,
    expre_uv_14d             int,
    clk_cnt_14d              int,
    clk_uv_14d               int,
    clk_rate_14d             double,
    cr_rate_14d              double,
    expre_cnt_per_u_14d      double,
    clk_cnt_per_u_14d        double,
    gmv_14d                  double,
    collect_cnt_14d          int,
    collect_uv_14d           int,
    add_cart_cnt_14d         int,
    add_cart_uv_14d          int,
    order_cnt_14d            int,
    order_uv_14d             int,
    sale_vol_14d             int,
    comment_cnt_14d          int,
    good_comment_cnt_14d     int,
    bad_comment_cnt_14d      int,
    avg_stay_time_14d        double,
    refund_order_cnt_14d     int,
    refund_amt_14d           double,
    expre_on_second_rate_14d double,
    clk_on_second_rate_14d   double,
    gmv_on_second_rate_14d   double,
    expre_on_brand_rate_14d  double,
    clk_on_brand_rate_14d    double,
    gmv_on_brand_rate_14d    double,
    expre_effciency_14d      double,
    gr_14d                   double,
    add_cart_on_mct_rate_14d double,
    collect_on_mct_rate_14d  double,
    expre_on_mct_rate_14d    double,
    clk_on_mct_rate_14d      double,
    gmv_on_mct_rate_14d      double,
    expre_cnt_30d            int,
    expre_uv_30d             int,
    clk_cnt_30d              int,
    clk_uv_30d               int,
    clk_rate_30d             double,
    cr_rate_30d              double,
    expre_cnt_per_u_30d      double,
    clk_cnt_per_u_30d        double,
    gmv_30d                  double,
    collect_cnt_30d          int,
    collect_uv_30d           int,
    add_cart_cnt_30d         int,
    add_cart_uv_30d          int,
    order_cnt_30d            int,
    order_uv_30d             int,
    sale_vol_30d             int,
    comment_cnt_30d          int,
    good_comment_cnt_30d     int,
    bad_comment_cnt_30d      int,
    avg_stay_time_30d        double,
    refund_order_cnt_30d     int,
    refund_amt_30d           double,
    expre_on_second_rate_30d double,
    clk_on_second_rate_30d   double,
    gmv_on_second_rate_30d   double,
    expre_on_brand_rate_30d  double,
    clk_on_brand_rate_30d    double,
    gmv_on_brand_rate_30d    double,
    expre_effciency_30d      double,
    gr_30d                   double,
    add_cart_on_mct_rate_30d double,
    collect_on_mct_rate_30d  double,
    expre_on_mct_rate_30d    double,
    clk_on_mct_rate_30d      double,
    gmv_on_mct_rate_30d      double,
    expre_cnt_mct_1d         int,
    clk_cnt_mct_1d           int,
    clk_rate_mct_1d          double,
    expre_uv_mct_1d          int,
    clk_uv_mct_1d            int,
    add_cart_cnt_mct_1d      int,
    collect_cnt_mct_1d       int,
    order_cnt_mct_1d         int,
    cr_rate_mct_1d           double,
    gmv_mct_1d               double,
    avg_stay_time_mct_1d     double,
    expre_cnt_mct_3d         int,
    clk_cnt_mct_3d           int,
    clk_rate_mct_3d          double,
    expre_uv_mct_3d          int,
    clk_uv_mct_3d            int,
    add_cart_cnt_mct_3d      int,
    collect_cnt_mct_3d       int,
    order_cnt_mct_3d         int,
    cr_rate_mct_3d           double,
    gmv_mct_3d               double,
    avg_stay_time_mct_3d     double,
    expre_cnt_mct_7d         int,
    clk_cnt_mct_7d           int,
    clk_rate_mct_7d          double,
    expre_uv_mct_7d          int,
    clk_uv_mct_7d            int,
    add_cart_cnt_mct_7d      int,
    collect_cnt_mct_7d       int,
    order_cnt_mct_7d         int,
    cr_rate_mct_7d           double,
    gmv_mct_7d               double,
    avg_stay_time_mct_7d     double,
    expre_cnt_mct_14d        int,
    clk_cnt_mct_14d          int,
    clk_rate_mct_14d         double,
    expre_uv_mct_14d         int,
    clk_uv_mct_14d           int,
    add_cart_cnt_mct_14d     int,
    collect_cnt_mct_14d      int,
    order_cnt_mct_14d        int,
    cr_rate_mct_14d          double,
    gmv_mct_14d              double,
    avg_stay_time_mct_14d    double,
    expre_cnt_mct_30d        int,
    clk_cnt_mct_30d          int,
    clk_rate_mct_30d         double,
    expre_uv_mct_30d         int,
    clk_uv_mct_30d           int,
    add_cart_cnt_mct_30d     int,
    collect_cnt_mct_30d      int,
    order_cnt_mct_30d        int,
    cr_rate_mct_30d          double,
    gmv_mct_30d              double,
    avg_stay_time_mct_30d    double,
    expre_cnt_second_1d      int,
    clk_cnt_second_1d        int,
    clk_rate_second_1d       double,
    expre_uv_second_1d       int,
    clk_uv_second_1d         int,
    cr_rate_second_1d        double,
    gmv_second_1d            double,
    expre_cnt_second_3d      int,
    clk_cnt_second_3d        int,
    clk_rate_second_3d       double,
    expre_uv_second_3d       int,
    clk_uv_second_3d         int,
    cr_rate_second_3d        double,
    gmv_second_3d            double,
    expre_cnt_second_7d      int,
    clk_cnt_second_7d        int,
    clk_rate_second_7d       double,
    expre_uv_second_7d       int,
    clk_uv_second_7d         int,
    cr_rate_second_7d        double,
    gmv_second_7d            double,
    expre_cnt_second_14d     int,
    clk_cnt_second_14d       int,
    clk_rate_second_14d      double,
    expre_uv_second_14d      int,
    clk_uv_second_14d        int,
    cr_rate_second_14d       double,
    gmv_second_14d           double,
    expre_cnt_second_30d     int,
    clk_cnt_second_30d       int,
    clk_rate_second_30d      double,
    expre_uv_second_30d      int,
    clk_uv_second_30d        int,
    cr_rate_second_30d       double,
    gmv_second_30d           double,
    is_order                 int,
    order_cnt                int,
    sale_vol                 int,
    is_add_cat               int,
    add_cart_cnt             int,
    is_recommend             int
) COMMENT '商品成交概率分模型特征' PARTITIONED BY (pt STRING)
    row format delimited fields terminated by '\001' STORED AS PARQUETFILE
    location 's3://vova-mlb/REC/data/base/mlb_vova_goods_performance_d/'
;
