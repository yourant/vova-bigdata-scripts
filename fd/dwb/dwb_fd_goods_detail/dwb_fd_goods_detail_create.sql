create table if not exists dwb.dwb_fd_goods_detail
(
    project_name                string comment '组织',
    country_code                string comment '国家',
    source_type                    string comment '平台类型H5，APP,PC',
    goods_id                    bigint comment '商品ID',
    virtual_goods_id            bigint comment '虚拟商品ID',
    cat_name                     string comment '品类名',
    goods_impression_session bigint comment '商品曝光会话',
    goods_click_session      bigint comment '商品点击会话',
    goods_add_session         bigint comment '商品加车会话',
    cy_po_impression           bigint comment '品类,预售列表曝光会话',
    cy_po_click                 bigint comment '品类,预售列表点击会话',
    detail_add                  bigint comment '详情页加车会话',
    detail_view                 bigint comment '详情页浏览会话',
    order_paid_number                 bigint comment '已支付订单数',
    goods_amount                 bigint comment '销售额'
)comment '商品明细表'
    partitioned by (pt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;