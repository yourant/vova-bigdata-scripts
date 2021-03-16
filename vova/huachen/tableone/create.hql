drop table tmp.merchant_data;
create external table if not exists tmp.merchant_data
(
    mct_id BIGINT COMMENT '商品所属商家',
    mct_name STRING COMMENT '商家名称',
    first_cat_name STRING COMMENT '商品一级类目名称',
    mct_gvm     DECIMAL(15,4) COMMENT '销售额',
    goods_number BIGINT COMMENT '子订单量',
    mct_gvm_shipped DECIMAL(15,4) COMMENT '已发货销售额',
    goods_number_shipped BIGINT COMMENT '已发货子订单量',
    price DECIMAL(15,4) COMMENT '笔单价',
    goods_sold_rate DECIMAL(15,2) COMMENT '商品动销率',
    goods_new_sold_rate DECIMAL(15,2) COMMENT '新品动销率',
    add_cart_cnt BIGINT COMMENT '加购商品数',
    cart_rate DECIMAL(15,2) COMMENT '加购转化率'
) COMMENT '商家数据';

/*
drop table tmp.sc;
create external table if not exists tmp.sc
(
    mct_id BIGINT COMMENT '商品所属商家',
    first_cat_name STRING COMMENT '商品一级类目名称',
    goods_sold_cnt BIGINT COMMENT '商品出单数'
) COMMENT '商品出单数量'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
;

drop table tmp.csc;
create external table if not exists tmp.csc
(
    mct_id BIGINT COMMENT '商品所属商家',
    first_cat_name STRING COMMENT '商品一级类目名称',
    goods_can_sale_cnt BIGINT COMMENT '商品在架单数'
) COMMENT '在架商品数'
;


drop table tmp.tmp_online_goods_new_cnt;
create external table if not exists tmp.tmp_online_goods_new_cnt
(
    mct_id BIGINT COMMENT '商品所属商家',
    first_cat_name STRING COMMENT '商品一级类目名称',
    goods_online_new_cnt BIGINT COMMENT '上新商品数'
) COMMENT '近一个月上新商品数'
;

drop table tmp.tmp_goods_new_sold_cnt;
create external table if not exists tmp.tmp_goods_new_sold_cnt
(
    mct_id BIGINT COMMENT '商品所属商家',
    first_cat_name STRING COMMENT '商品一级类目名称',
    goods_sold_new_cnt BIGINT COMMENT '出单新商品数'
) COMMENT '近一个月上新商品出单数'
;

drop table tmp.tmp_add_cart;
create external table if not exists tmp.tmp_add_cart
(
    mct_id BIGINT COMMENT '商品所属商家',
    first_cat_name STRING COMMENT '商品一级类目名称',
    add_cat_cnt BIGINT COMMENT '加购商品数'
) COMMENT '加购商品数'
;

drop table tmp.tmp_cart_uv;
create external table if not exists tmp.tmp_cart_uv
(
    mct_id BIGINT COMMENT '商品所属商家',
    first_cat_name STRING COMMENT '商品一级类目名称',
    cart_uv BIGINT COMMENT '加购商品UV'
) COMMENT '加购商品UV'
;


drop table tmp.tmp_ex_uv;
create external table if not exists tmp.tmp_ex_uv
(
    mct_id BIGINT COMMENT '商品所属商家',
    first_cat_name STRING COMMENT '商品一级类目名称',
    expre_uv BIGINT COMMENT '商品曝光UV'
) COMMENT '商品曝光UV'
;*/
