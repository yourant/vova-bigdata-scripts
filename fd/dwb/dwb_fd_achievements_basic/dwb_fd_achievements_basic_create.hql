create table if not exists dwb.dwb_fd_achievements_basic
(
    cat_name                string          comment '商品类目名',
    country_code            string          comment '国家',
    goods_id                string          comment '商品id',
    cat_id                  string          comment '商品品类ID',
    shop_price              decimal(15, 2)  comment '商品售价',
    goods_number            bigint          comment '商品件数',
    order_time_original     string          comment '订单时间',
    project_name            string          comment '组织',
    finder                  string          comment '选款人',
    test_finish_dt          string          comment '测款结束日期'
)comment '绩效基础表'
    partitioned by (`pt` string)
    row format delimited fields terminated by '\001'
    stored as parquet;
