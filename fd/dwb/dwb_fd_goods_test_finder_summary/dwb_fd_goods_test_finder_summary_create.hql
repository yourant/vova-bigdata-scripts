create table if not exists dwb.dwb_fd_goods_test_finder_summary
(
    project_name                  STRING comment "组织",
    finder                        STRING comment "选款人",
    cat_id                        BIGINT comment "分类ID",
    cat_name                      STRING comment "分类名",
    test_type                     STRING comment "测款类型",
    preorder_plan_name            STRING comment "预售计划名称",
    finished_goods_num            BIGINT comment "测款结束的商品",
    success_goods_num             BIGINT comment "测款成功的商品",
    success_goods_sales_amount_7d DECIMAL(15, 4) comment "测款成功商品7天销售额",
    cat_sales_amount_7d     DECIMAL(15, 4) comment "分类7天销售额"
)
    comment "最近一个月选款人的测试情况"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUET;