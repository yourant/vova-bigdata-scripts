
create table if not exists dwb.dwb_fd_goods_test_finder_rpt
(
    project_name                  STRING comment "组织",
    finder                        STRING comment "选款人",
    test_time                     String comment "测款时间",
    cat_name                      STRING comment "分类名",
    test_type                     STRING comment "测款类型",
    preorder_plan_name            STRING comment "预售计划名称",
    finished_goods_num            BIGINT comment "测款结束商品数",
    success_goods_num             BIGINT comment "测款成功商品数",
    success_goods_sales_amount_7d DECIMAL(15, 4) comment "测款成功商品近7日的销售额",
    cat_sales_amount_7d           DECIMAL(15, 4) comment "近7日同品类所有商品销售额",
    hot_style_num                 bigint     comment '爆款商品数量'
)
    comment "选款人测款成功商品报表"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUET;