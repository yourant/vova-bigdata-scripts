create table if not exists dwd.dwd_fd_goods_test_thread_single
(
    test_thread_id   bigint comment '测款线程ID',
    goods_id         bigint comment '商品id',
    virtual_goods_id bigint comment '商品虚拟ID',
    pipeline_id      bigint comment '测款管道ID',
    project_name     string comment '组织',
    platform_name    string comment '平台，APP、H5、PC',
    country_code     string comment '国家',
    cat_id           bigint comment '品类id',
    cat_name         string comment '品类名字',
    state            bigint comment '状态',
    type_id          bigint comment '类型id',
    result           bigint comment '测款结果',
    reason           string comment '测款添加理由',
    goods_type       string comment '商品类型',
    goods_source     string comment '商品来源',
    test_count       bigint comment '测款次数',
    create_time      string comment '线程创建时间',
    test_time        string comment '入测时间',
    finish_time      string comment '线程结束时间'
)
    comment "单线程快速测款的商品明细表"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;