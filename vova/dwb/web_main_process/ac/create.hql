DROP TABLE dwb.dwb_ac_web_main_process;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_ac_web_main_process
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
    gmv                          decimal(15, 2) COMMENT 'gmv',
    is_new_reg_time                          string COMMENT 'is_new_reg_time',
    is_new_register_success_time                          string COMMENT 'is_new_register_success_time',
    datasource                   string COMMENT 'datasource'
) COMMENT 'web主流程' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
