create table if not exists dwd.dwd_fd_finished_preorder
(
    preorder_plan_id   bigint comment '商品预售计划ID',
    preorder_plan_name STRING comment '商品预售计划名称',
    goods_id           bigint comment '商品id',
    virtual_goods_id   bigint comment '商品虚拟ID',
    project_name       string comment '组织',
    cat_id             bigint comment '品类id',
    cat_name           string comment '品类名字',
    result             bigint comment '测款结果',
    finish_time        string comment '线程结束时间',
    test_time          string comment '入测时间'
)
    comment "已经结束的预售信息"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;