create external table if not exists dwb.dwb_fd_goods_picture_uv
(
    goods_id                string          comment '商品ID',
    project                 string          comment '组织',
    platform                string          comment '平台',
    country                 string          comment '国家',
    rtype                   string          comment '记录类型',
    list_type               string          comment '列表类型',
    picture_group           string          comment '图片组',
    picture_batch           string          comment '图片批次',
    uv                      bigint          comment 'uv值'
) PARTITIONED BY (
    `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


