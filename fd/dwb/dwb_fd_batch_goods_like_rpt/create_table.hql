
CREATE  TABLE IF NOT EXISTS dwb.dwb_fd_batch_goods_like_rpt
(
    batch            string comment '批次号',
    virtual_goods_id string comment '虚拟商品ID',
    project          string comment '站点',
    country          string comment '国家',
    platform_type    string comment '平台',
    like_num         bigint comment '喜欢数量',
    unlike_num       bigint comment '不喜欢数量',
    impressions      bigint comment '曝光数'
) COMMENT '近60天各批次号下商品表现报表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS orc;