drop table if exists ads.ads_vova_comment_favourable;
create table if not exists ads.ads_vova_comment_favourable
(
    goods_id      bigint COMMENT '商品ID',
    buyer_id     bigint COMMENT 'BUYER id',
    comment_id      bigint COMMENT 'comment id',
    mct_id       bigint COMMENT 'merchant id',
    first_cat_id  int COMMENT '一级品类id',
    rank          bigint COMMENT 'd_排名',
    order_type int COMMENT 'order type 0: sales_number 1:ctr'
) COMMENT 'favourable comment' PARTITIONED BY (pt string)
    STORED AS PARQUETFILE;
;