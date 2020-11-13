CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_preorder_plan
(
    id                         bigint comment '自增主键',
    plan_id                    bigint comment '商品预售计划ID',
    plan_name                  string comment '预售计划名称',
    project_name               string comment '组织(多组织用逗号隔开)',
    country_code               string comment '国家(多国家用逗号隔开)',
    platform                   string comment '预售平台',
    delivery_period_min        bigint comment '最小交期天数',
    delivery_period_max        bigint comment '最大交期天数',
    delivery_period_start_date bigint comment '交期开始时间戳',
    delivery_period_end_date   bigint comment '交期结束时间戳',
    preorder_start_time        bigint comment '预售开始时间戳',
    preorder_end_time          bigint comment '预售结束时间戳',
    product_ids                string comment '预售商品ID列表(逗号分隔,先后顺序即展示顺序)',
    create_time                bigint comment '计划创建时间',
    is_delete                  bigint comment '是否删除',
    reason                     string comment '商品测款理由',
    is_into_group              bigint comment '是否进入预售组'
) comment '从vbridal同步过来的测款计划表'
    STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods_preorder_plan
select `(dt)?+.+`
from ods_fd_vb.ods_fd_goods_preorder_plan_arc
where dt = '${hiveconf:dt}';
