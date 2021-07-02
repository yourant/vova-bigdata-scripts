create table if not exists dwb.dwb_fd_commission_summary
(   
    project_name            string          comment '组织',
    cat_name                string          comment '商品类目名',
    project                 string          comment '项目',
    finder                  string          comment '选品人',
    commission_goods_number bigint          comment '提成商品数',
    commission_goods_amount decimal(15, 2)  comment '提成金额'
)comment '提成汇总表'
    partitioned by (`pt` string)
    row format delimited fields terminated by '\001'
    stored as parquet;