drop table dwd.dwd_vova_fact_web_start_up;
CREATE TABLE IF NOT EXISTS dwd.dwd_vova_fact_web_start_up
(
    datasource     string comment '数据平台',
    domain_userid  string COMMENT '设备ID',
    buyer_id      bigint COMMENT '设备对应用户ID',
    region_code   string COMMENT 'geo_country',
    first_page_url   string COMMENT 'page_url',
    first_referrer   string COMMENT 'referrer',
    min_create_time  TIMESTAMP COMMENT '当日登录最小时间',
    max_create_time  TIMESTAMP COMMENT '当日登录最大时间'
) COMMENT 'web用户访问表' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table dim.dim_vova_web_domain_userid;
CREATE TABLE IF NOT EXISTS dim.dim_vova_web_domain_userid
(
    domain_userid  string COMMENT 'domain_userid',
    buyer_id       bigint COMMENT '设备对应用户ID',
    activate_time  TIMESTAMP COMMENT '激活时间',
    first_order_id bigint COMMENT 'first_order_id',
    first_pay_time TIMESTAMP COMMENT 'first_pay_time',
    medium         string COMMENT 'medium',
    source         string COMMENT 'source',
    reg_time         TIMESTAMP COMMENT '用户注册时间',
    register_success_time         TIMESTAMP COMMENT '打点数据点击注册按钮返回成功时间'
) COMMENT 'web domain_userid作为unique key'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table tmp.tmp_vova_web_main_process_register;
CREATE TABLE tmp.tmp_vova_web_main_process_register
(
    datasource      STRING COMMENT '数据平台',
    domain_userid   STRING COMMENT '',
    min_create_time TIMESTAMP COMMENT '当日登录最小时间'
) COMMENT '点击注册按钮返回成功'
  PARTITIONED BY (pt STRING);


DROP TABLE rpt.rpt_vova_web_main_process;
CREATE TABLE IF NOT EXISTS rpt.rpt_vova_web_main_process
(
    action_date                  date,
    region_code                  string,
    is_activate                  string,
    is_new_user                  string,
    medium                       string,
    source                       string,
    dau                          bigint,
    homepage_uv                  bigint,
    product_detail_uv            bigint COMMENT '商详页曝光',
    product_detail_pv            bigint COMMENT '商详页曝光',
    add_to_cart_uv               bigint COMMENT '商详页加购按钮点击',
    add_to_cart_success_uv       bigint COMMENT '加购成功（服务端返回商详页加购成功）',
    add_to_cart_success_pv       bigint COMMENT '加购成功（服务端返回商详页加购成功）',
    cart_uv                      bigint COMMENT '购物车页面曝光',
    checkout_common_click_uv     bigint COMMENT '购物车页面确认按钮',
    checkout_apply_success_uv    bigint COMMENT '购物车页面转化成功',
    checkout_page_view_uv        bigint COMMENT 'checkout 页面',
    place_order_uv               bigint COMMENT 'checkout 页面确认按钮',
    place_order_apply_success_uv bigint COMMENT 'checkout 转化成功',
    wishlist_uv                  bigint COMMENT 'wishlist页面曝光',
    wishlist_goods_impression_uv bigint COMMENT 'wishlist页面商品曝光',
    wishlist_goods_click_uv      bigint COMMENT 'wishlist页面商品点击',
    bag_uv                       bigint COMMENT '商详页加购后悬浮窗曝光',
    bag_viewchart_uv             bigint COMMENT '商详页加购后悬浮窗 view chart 点击',
    bag_checkout_uv              bigint COMMENT '商详页加购后悬浮窗 check out 点击',
    order_uv                     bigint COMMENT '下单uv',
    pay_uv                       bigint COMMENT '支付uv',
    cur_pay_uv                   bigint COMMENT '当日下单支付uv',
    pay_order_cnt                bigint COMMENT '支付订单uv',
    sale_goods_cnt               bigint COMMENT '销量',
    gmv                          decimal(15, 2) COMMENT 'gmv'
) COMMENT 'web主流程' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
