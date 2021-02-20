-- 用户画像界面展示数据（由于数据量很大查询数仓，所以提前预处理好）
drop table if exists ads.ads_vova_goods_comment;
create external table if  not exists  ads.ads_vova_goods_comment (
    `goods_id`                      bigint        COMMENT 'i_商品id',
    `comment_id`                    bigint        COMMENT 'd_评论id',
    `rating`                        int           COMMENt 'i_评分',
    `comment`                       string        COMMENT 'i_评论类容',
    `language_code`                 string        COMMENT 'i_评论语言',
    `post_time`                     timestamp     COMMENT 'i_评论时间',
    `tag`                           string        COMMENT 'i_标签'
)comment '页面展示查询评论' PARTITIONED BY (gpt string)
     STORED AS PARQUETFILE;