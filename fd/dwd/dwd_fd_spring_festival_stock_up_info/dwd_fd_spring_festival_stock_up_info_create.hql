create table if not exists dwd.dwd_fd_spring_festival_stock_up_info (
    stock_up_id bigint comment '春节备货id'
    ,goods_id bigint comment '商品id'
    ,small_cat string comment '小分类'
    ,season bigint comment '季节'
    ,start_date string comment '备货开始时间'
    ,end_date string comment '备货结束时间'
    ,node_time string comment '备货时间结点'
    ,source bigint comment '记录来源'
    ,remark string comment '备注'
) comment '春节备货库存表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS PARQUET
;