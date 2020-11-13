CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_test_reports_last_arc
(
    id                    bigint comment '自增主键',
    project_name          string comment '组织名',
    test_type             bigint comment '测试类型',
    test_state            bigint comment '测试状态',
    test_result           bigint comment '测试结果',
    virtual_goods_id      bigint comment '商品虚拟id',
    goods_id              bigint comment '商品真实id',
    platform              string comment '测款平台',
    cat_id                bigint comment '',
    country               string comment '测款国家',
    impressions           bigint comment 'impressions',
    users                 bigint comment '用户',
    orders                bigint comment ' ',
    clicks                bigint comment ' ',
    cart                  bigint comment ' ',
    view_cart             bigint comment ' ',
    ctr                   decimal(15, 4) comment ' ',
    rate                  bigint comment ' ',
    cr                    bigint comment ' ',
    cat_rate              decimal(15, 4) comment ' ',
    impressions_threshold bigint comment ' ',
    cr_threshold          decimal(15, 4) comment ' ',
    ctr_threshold         decimal(15, 4) comment ' ',
    order_threshold       bigint comment ' ',
    enter_time            bigint comment '入队时间',
    update_time           bigint comment '最后更新时间',
    test_count            bigint comment '测款次数'
) comment '最后商品测试报告'
    PARTITIONED BY (dt STRING )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001 '
    STORED AS PARQUETFILE;


INSERT overwrite table ods_fd_vb.ods_fd_goods_test_reports_last_arc PARTITION (dt = '${hiveconf:dt}')
select id,
       project_name,
       test_type,
       test_state,
       test_result,
       virtual_goods_id,
       goods_id,
       platform,
       cat_id,
       country,
       impressions,
       users,
       orders,
       clicks,
       cart,
       view_cart,
       ctr,
       rate,
       cr,
       cart_rate,
       impressions_threshold,
       cr_threshold,
       ctr_threshold,
       order_threshold,
       unix_timestamp(date_format(from_utc_timestamp(to_utc_timestamp(enter_time,'America/Los_Angeles'),'UTC'),'yyyy-MM-dd HH:mm:ss')) as enter_time,
       unix_timestamp(date_format(from_utc_timestamp(to_utc_timestamp(update_time,'America/Los_Angeles'),'UTC'),'yyyy-MM-dd HH:mm:ss')) as update_time,
       test_count
from tmp.tmp_fd_goods_test_reports_last_full;
