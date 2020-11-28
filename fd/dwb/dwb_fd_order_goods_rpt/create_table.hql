CREATE table if not exists  dwb.dwb_fd_order_goods_rpt
(
        order_id bigint,
        user_id bigint,
        platform_type string,
        pay_status bigint,
        country_code string,
        party_id bigint,
        project_name string,
        order_amount decimal(15,4),
        shipping_fee decimal(15,4),
        goods_id bigint,
        cat_id bigint,
        cat_name string,
        goods_number bigint,
        shop_price decimal(15,4),
        virtual_goods_id bigint,
        email bigint,
        goods_amount double,
        order_time string,
        pay_time string
)comment '每日商品销售额/销量明细表'
partitioned by(`pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS orc
TBLPROPERTIES ("orc.compress"="SNAPPY");