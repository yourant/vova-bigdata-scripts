create external table if not exists dwb.dwb_fd_goods_element_uv
(
    goods_id                string          comment '商品ID',
    project                 string          comment '组织',
    platform                string          comment '平台',
    country                 string          comment '国家',
    rtype                   string          comment '记录类型',
    page_code               string          comment '页面标识',
    element_name            string          comment '元素名称',
    element_tag             string          comment '元素标签',
    element_batch           string          comment '元素批次',
    uv                      bigint          comment 'uv值'
) PARTITIONED BY (
    `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
