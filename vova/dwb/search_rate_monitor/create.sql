drop table dwb.dwb_vova_search_view_monitor_1;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_search_view_monitor_1
(
    cur_date               string comment 'd_日期',
    home_page_top_clk_cnt  bigint COMMENT 'i_首页-顶部搜索tab点击人数',
    sort_enter_top_clk_cnt bigint COMMENT 'i_分类入口-顶部搜索tab点击人数',
    search_begin_uv        bigint COMMENT 'i_搜索开始页访问人数',
    hot_words_expre_cnt    bigint COMMENT 'i_热搜词曝光人数',
    hot_words_clk_cnt      bigint COMMENT 'i_热搜词点击人数',
    lenovo_words_expre_cnt bigint COMMENT 'i_联想词曝光人数',
    lenovo_words_clk_cnt   bigint COMMENT 'i_联想词点击人数'
) COMMENT '搜索开始页模块监控' PARTITIONED BY (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table dwb.dwb_vova_search_view_monitor_2;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_search_view_monitor_2
(
    cur_date       string comment 'd_日期',
    goods_list     string COMMENT 'd_首页-顶部搜索tab点击人数',
    search_pv      bigint COMMENT 'i_进行搜索人数',
    search_uv      bigint COMMENT 'i_进行搜索次数',
    driv_search_pv bigint COMMENT 'i_主动搜索人数',
    driv_search_uv bigint COMMENT 'i_主动搜索次数',
    goods_expre_pv bigint COMMENT 'i_商品曝光人数',
    goods_expre_uv bigint COMMENT 'i_商品曝光次数',
    cart_uv        bigint COMMENT 'i_商品加购人数',
    cart_pv        bigint COMMENT 'i_商品加购次数',
    pay_uv         bigint COMMENT 'i_商品支付人数',
    gmv            decimal COMMENT 'i_gmv',
    cart_rate      string COMMENT 'i_加购率',
    pay_rate       string COMMENT 'i_支付率',
    gmv_cr         string COMMENT 'i_gmv_cr'
) COMMENT '搜索结果商品曝光情况' PARTITIONED BY (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table dwb.dwb_vova_search_view_monitor_3;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_search_view_monitor_3
(
    cur_date    string comment 'd_日期',
    filter_name string COMMENT 'd_筛选项目',
    expre_pv    bigint COMMENT 'i_曝光人数',
    expre_uv    bigint COMMENT 'i_曝光次数',
    clk_pv      bigint COMMENT 'i_点击人数',
    clk_uv      bigint COMMENT 'i_点击次数',
    ctr         string comment 'i_ctr'
) COMMENT '搜索结果页过滤器使用情况' PARTITIONED BY (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;