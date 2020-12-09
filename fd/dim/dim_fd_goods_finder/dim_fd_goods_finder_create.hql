CREATE EXTERNAL TABLE IF NOT EXISTS `dim.dim_fd_goods_finder`
(
    goods_id         bigint comment '商品id',
    virtual_goods_id bigint comment '商品虚拟id',
    project_name     string comment '组织名',
    finder           string comment '选款人'
) COMMENT "商品对应选款人"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUET;