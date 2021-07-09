create table if not exists dwb.dwb_fd_reach_achievements_standard_goods_info
(   
    project_name            string          comment '组织',
    cat_name                string          comment '商品类目名',
    project                 string          comment '项目',
    finder                  string          comment '选品人',
    goods_id                string          comment '商品id',
    order_time_original     string          comment '达标日期',
    goods_amount            decimal(15, 2)  comment '达标日销额'
)comment '商品达标明细表'
    partitioned by (`pt` string)
    row format delimited fields terminated by '\001'
    stored as parquet;