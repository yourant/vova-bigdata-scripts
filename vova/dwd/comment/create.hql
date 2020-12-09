drop table dwd.dwd_vova_fact_comment;
CREATE TABLE IF NOT EXISTS dwd.dwd_vova_fact_comment
(
    datasource STRING COMMENT '数据平台',
    comment_id     bigint COMMENT '评论id',
    goods_id       bigint COMMENT 'goods_id',
    cat_id         bigint COMMENT 'cat_id',
    order_goods_id bigint COMMENT 'order_goods_id',
    buyer_id       bigint COMMENT 'buyer_id',
    title          STRING COMMENT 'title',
    comment        STRING COMMENT 'comment',
    rating         bigint COMMENT '评分 1 2 3 4 5',
    status         STRING COMMENT 'OK,REJECTED,DELETED,DNR',
    post_time  timestamp COMMENT '评论时间',
    type           STRING COMMENT '评论类型',
    mct_id         bigint COMMENT '商家id',
    display_order  bigint COMMENT '排序',
    language_id    bigint COMMENT '评论语言id',
    tag STRING COMMENT '用户评论选择的标签的语言包code'
) COMMENT '评论事实表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



