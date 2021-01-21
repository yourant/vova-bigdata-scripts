DROP TABLE dwb.dwb_vova_self_operated_merchant;


CREATE external TABLE dwb.dwb_vova_self_operated_merchant
(
    action_date               date COMMENT 'd_日期',
    datasource                STRING COMMENT 'd_datasource',
    region_code               STRING COMMENT 'd_国家',
    platform                  STRING COMMENT 'd_平台',
    first_cat_name            STRING COMMENT 'd_一级品类',
    goods_id                  STRING COMMENT 'd_goods_id',
    gmv                       decimal(15, 2) COMMENT 'i_gmv',
    shipping_fee              decimal(15, 2) COMMENT 'i_预期运费',
    bonus                     decimal(15, 2) COMMENT 'i_平台补贴金额',
    purchase_amount           decimal(15, 2) COMMENT '',
    paid_order_cnt            bigint COMMENT 'i_支付成功订单量',
    paid_buyer_cnt            bigint COMMENT 'i_购买用户数',
    cur_sale_goods_cnt        bigint COMMENT 'i_销售商品数（种）',
    cur_sale_goods_number_cnt bigint COMMENT 'i_已支付商品销量',
    refund_amount             decimal(15, 2) COMMENT 'i_退款金额',
    on_sale_goods_cnt         bigint COMMENT 'i_在架商品数',
    impression bigint COMMENT 'i_曝光量',
    click bigint COMMENT '',
    self_mct_name string COMMENT 'i_店铺名',
    is_brand string COMMENT 'd_是否品牌商品',
    confirm_order_cnt  bigint COMMENT 'i_确认订单数'
) COMMENT '自营店铺数据监控-商品商家数据' PARTITIONED BY (pt STRING)
   STORED AS PARQUETFILE;

CREATE external TABLE dwb.dwb_vova_self_operated_merchant_confirm_order(
  `action_date` date COMMENT 'd_日期',
  `datasource` string COMMENT 'd_datasource',
  `region_code` string COMMENT 'd_国家',
  `platform` string COMMENT 'd_平台',
  `first_cat_name` string COMMENT 'd_一级品类',
  `self_mct_name` string COMMENT 'i_店铺名',
  `is_brand` string COMMENT 'd_是否品牌商品',
  `confirm_order_cnt` bigint COMMENT 'd_确认订单数')
COMMENT '自营店铺数据监控-商品商家数据-确认订单'  PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE;


CREATE external TABLE dwb.dwb_vova_self_merchant(
  `event_date` date COMMENT 'd_日期',
  `datasource` string COMMENT 'd_datasource',
  `region_code` string COMMENT 'd_国家',
  `platform` string COMMENT 'd_平台',
  `first_cat_name` string COMMENT 'd_一级品类',
  `gmv` decimal(15,4) COMMENT 'i_gmv',
  `purchase_total_amount` decimal(15,4) COMMENT '',
  `purchase_order_goods_id_order_0` bigint COMMENT 'i_待拍单',
  `purchase_order_goods_id_order_1` bigint COMMENT 'i_已拍单',
  `purchase_order_goods_id_order_2` bigint COMMENT 'i_已取消',
  `purchase_order_goods_id_order_4` bigint COMMENT 'i_拍单失败',
  `purchase_order_goods_id_pay_0` bigint COMMENT 'i_待付款',
  `purchase_order_goods_id_pay_1` bigint COMMENT 'i_已付款',
  `purchase_order_goods_id_ship_0` bigint COMMENT 'i_pdd待发货',
  `purchase_order_goods_id_ship_1` bigint COMMENT 'i_pdd已发货',
  `collect_0` bigint COMMENT 'i_集货仓待收货',
  `collect_1` bigint COMMENT 'i_集货仓已收货',
  `collect_2` bigint COMMENT 'i_集货仓已发货',
  `order_goods_cnt` bigint COMMENT 'i_销售渠道总订单数',
  `purchase_order_goods_cnt` bigint COMMENT 'i_采购渠道总订单数',
  `self_mct_name` string COMMENT 'd_店铺名',
  `is_brand` string COMMENT 'd_是否品牌商品')
COMMENT '自营店铺数据监控-订单主流程报表'  PARTITIONED BY (pt STRING)
STORED AS PARQUETFILE;



CREATE external TABLE dwb.dwb_vova_selfshop_added_ana(
  `cur_date` string COMMENT '事件发生日期',
  `datasource` string COMMENT '来源',
  `region_code` string COMMENT '国家编码',
  `platform` string COMMENT '平台',
  `first_cat_name` string COMMENT '商品一级类目',
  `is_brand` string COMMENT '品牌',
  `impression` bigint COMMENT '曝光',
  `clks` bigint COMMENT '点击',
  `ctr` decimal(7,4) COMMENT 'ctr',
  `expouse_uv` bigint COMMENT '曝光uv',
  `click_uv` bigint COMMENT '点击uv',
  `gmv` decimal(15,2) COMMENT 'gmv',
  `expouse_income` string COMMENT '曝光收益',
  `cart` bigint COMMENT '加购数',
  `cart_uv` bigint COMMENT '加购uv',
  `cart_rate` string COMMENT '加购率',
  `order_uv` bigint COMMENT '下单uv',
  `order_count` bigint COMMENT '下单数',
  `total_sales` bigint COMMENT '商品销量',
  `order_rate` string COMMENT '下单率',
  `pay_nums` bigint COMMENT '支付数',
  `_uv` bigint COMMENT '支付uv',
  `pay_rate` string COMMENT '支付率',
  `total_change_rate` string COMMENT '总转化率',
  `rebuy_uv` bigint COMMENT '复购uv',
  `rebay_rate` string COMMENT '复购率')
COMMENT '店铺表现数据'  PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;


CREATE external TABLE dwb.dwb_vova_shop_com(
  `cur_date` string COMMENT '事件发生日期',
  `datasource` string COMMENT '来源',
  `region_code` string COMMENT '国家编码',
  `platform` string COMMENT '平台',
  `first_cat_name` string COMMENT '商品一级类目',
  `is_brand` string COMMENT '品牌',
  `impression_persent` string COMMENT '曝光占比',
  `clks_persent` string COMMENT '点击占比',
  `gmv_persent` string COMMENT 'gmv占比',
  `ctr_diff` string COMMENT 'ctr差异',
  `impression_diff` bigint COMMENT '曝光收益差异',
  `bestselling_persent` string COMMENT 'bestselling占比',
  `yourlike_persent` string COMMENT '猜你喜欢占比',
  `result_persent` string COMMENT '搜索结果占比',
  `cart_persent` string COMMENT '加购占比',
  `order_persent` string COMMENT '订单占比',
  `pay_persent` string COMMENT '支付占比')
COMMENT '店铺大盘对比数据'  PARTITIONED BY (pt STRING) STORED AS PARQUETFILE;


