drop table if exists dwb.dwb_expre_and_ord_his;
CREATE external TABLE IF NOT EXISTS dwb.dwb_expre_and_ord_his
(
    datasource                  string COMMENT 'd_datasource',
    ctry                        string COMMENT 'd_国家',
    os_type                     string COMMENT 'd_平台',
    main_channel                string COMMENT 'd_渠道',
    is_new                      string COMMENT 'd_是否新用户',
    dau                         bigint COMMENT 'i_dau',
    homepage_dau                bigint COMMENT 'i_首页dau',
    pd_uv                       bigint COMMENT 'i_详情页UV',
    cart_uv                     bigint COMMENT 'i_购物车页UV',
    cart_success_uv             bigint COMMENT 'i_加购成功UV',
    ordered_user_num            bigint COMMENT 'i_订单总人数',
    payed_user_num              bigint COMMENT 'i_支付成功UV',
    checkout_uv                 bigint COMMENT 'i_checkout页UV',
    homepage_nav_uv             bigint COMMENT 'i_首页类目导航UV',
    homepage_pop_uv             bigint COMMENT '首页推荐商品曝光UV',
    category_uv                 bigint COMMENT 'i_搜索类目导航UV',
    sear_begin_uv               bigint COMMENT 'i_搜索起始页UV',
    sear_result_uv              bigint COMMENT 'i_搜索结果页UV',
    pro_list_uv                 bigint COMMENT 'i_类目商品列表页UV'
) COMMENT '曝光订单统计报表'
PARTITIONED BY ( pt string) STORED AS PARQUETFILE;


alter table dwb.dwb_expre_and_ord_his add columns(payment_uv bigint comment 'payment页uv') cascade;
alter table dwb.dwb_expre_and_ord_his add columns(try_payment_uv bigint comment '尝试支付uv') cascade;


Drop table dwb.dwb_vova_element_device_uv;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_element_device_uv (
datasource                string                COMMENT 'd_datasource',
ctry                      string                COMMENT 'd_国家',
os_type                   string                COMMENT 'd_平台',
main_channel              string                COMMENT 'd_渠道',
is_new                    string                COMMENT 'd_是否新用户',
element_device_uv         bigint                COMMENT 'i_商品UV',
selling_uv                bigint                COMMENT 'i_可售商品UV',
flashsale_uv              bigint                COMMENT 'i_FlashSale商品UV',
add_cart_uv               bigint                COMMENT 'i_商品加车UV',
also_like_impressions_uv  bigint                COMMENT 'i_猜你喜欢商品曝光UV',
also_like_click_uv        bigint                COMMENT 'i_猜你喜欢商品点击UV',
colorsize_click_uv        bigint                COMMENT 'i_ColorSize点击UV',
buying_uv                 bigint                COMMENT 'i_商品购买发起UV',
comfirm_buy_uv            bigint                COMMENT 'i_商品购买确认UV'
) COMMENT '商详页商品流量监控' PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE;