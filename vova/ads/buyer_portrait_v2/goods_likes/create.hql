-- 用户画像商品好统计表
drop table if exists ads.ads_vova_buyer_portrait_goods_likes;
create external table if  not exists  ads.ads_vova_buyer_portrait_goods_likes (
    `buyer_id`                    bigint        COMMENT 'd_买家id',
    `gs_id`                       bigint        COMMENT 'd_商品id',
    `expre_cnt_1w`                bigint        COMMENT 'i_近7天用户goods_id曝光次数',
    `clk_cnt_1m`                  bigint        COMMENT 'i_近30天用户goods_id点击次数',
    `clk_valid_cnt_2m`            bigint        COMMENT 'i_近30天用户goods_id有效点击次数',
    `collect_cnt_2w`              bigint        COMMENT 'i_近60天用户goods_id收藏次数',
    `ord_cnt_6m`                  bigint        COMMENT 'i_近180天用户goods_id购买次数',
    `rating`                      decimal(13,4) COMMENT 'i_rating'
)COMMENT '用户画像商品好统计表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;
