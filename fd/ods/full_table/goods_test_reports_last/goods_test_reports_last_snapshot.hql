CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_test_reports_last
(
    id                    bigint comment '自增主键',
    project_name          string comment '组织名',
    test_type             bigint comment '测试类型',
    test_state            bigint comment '测试状态',
    test_result           bigint comment '测试结果',
    virtual_goods_id      bigint comment '商品虚拟id',
    goods_id              bigint comment '商品真实id',
    platform              string comment '测款平台',
    cat_id                int comment '',
    country               string comment '测款国家',
    impressions           bigint comment 'impressions',
    users                 int comment '',
    orders                int comment ' ',
    clicks                int comment ' ',
    cart                  int comment ' ',
    view_cart             int comment ' ',
    ctr                   float comment ' ',
    rate                  int comment ' ',
    cr                    int comment ' ',
    cat_rate              float comment ' ',
    impressions_threshold int comment ' ',
    cr_threshold          float comment ' ',
    ctr_threshold         float comment ' ',
    order_threshold       int comment ' ',
    enter_time            bigint comment '入队时间',
    update_time           bigint comment '最后更新时间',
    test_count            bigint comment '测款次数'
) comment '最后商品测试报告'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUETFILE
    TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods_test_reports_last
select `(dt)?+.+`
from ods_fd_vb.ods_fd_goods_test_reports_last_arc
where dt = '${hiveconf:dt}';
