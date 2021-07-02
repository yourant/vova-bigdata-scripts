create table if not exists dwb.dwb_fd_commission_standard 
(   
    project_name            string          comment '组织',
    cat_name                string          comment '商品类目名',
    project                 string          comment '项目',
    commission              bigint          comment '提成单价',
    threshold               decimal(15, 2)  comment '日销售额阈值',
    ordinal                 bigint          comment '商品序数阈值',
    dynamic_goods_num       bigint          comment '动销商品数'
)comment '提成标准表'
    partitioned by (`pt` string)
    row format delimited fields terminated by '\001'
    stored as parquet;