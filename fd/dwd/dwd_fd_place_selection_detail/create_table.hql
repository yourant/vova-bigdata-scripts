
create table if not exists dwd.dwd_fd_place_selection_detail(

    id bigint COMMENT '',
    goods_id bigint COMMENT '商品id',
    country_code string COMMENT '',
    project_name string COMMENT '',
    platform string COMMENT '',
    impressions bigint COMMENT '列表展示',
    clicks bigint COMMENT '列表点击',
    users bigint COMMENT '详情访问',
    sales_order bigint COMMENT '销量排序',
    detail_add_cart bigint COMMENT '商品详情页加车',
    list_add_cart bigint COMMENT '列表页加车',
    checkout bigint COMMENT '支付',
    sales_order_in_7_days bigint COMMENT '7天销售量',
    virtual_sales_order bigint COMMENT '',
    goods_order bigint COMMENT '',
    start_time timestamp COMMENT '',
    end_time timestamp COMMENT '',
    is_active bigint COMMENT '',
    last_update_time timestamp COMMENT '',
    sales bigint COMMENT '商品销量即销售件数',
    cat_name string,
    virtual_goods_id bigint
)comment'商品近14天表现数据明细表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS orc
    TBLPROPERTIES ("orc.compress"="SNAPPY");