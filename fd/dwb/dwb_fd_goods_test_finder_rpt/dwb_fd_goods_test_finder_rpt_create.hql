CREATE TABLE IF NOT EXISTS dwb.dwb_fd_goods_test_finder_rpt
(
    project                     string comment '项目',
    cat_name                    string comment '商品品类名',
    test_type                   string comment '商品测款类型',
    finder                      string comment '选款人',
    preorder_plan_name          string comment '预售计划名称',
    virtual_goods_id            bigint comment '商品虚拟id',
    test_finish_dt              string comment '测款结束日期',
    result                      bigint comment '测款结果,1表示成功',
    last_7_days_goods_sales     decimal(16, 6) comment '最近七天的商品销售额',
    last_7_days_cat_sales decimal(16, 6) comment '最近七天品类商品销售额'
) comment '根据选款人的测款商品汇总表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUET;