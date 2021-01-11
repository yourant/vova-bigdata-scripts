CREATE  TABLE if not exists dwd.dwd_fd_batch_detail
(
    batch            string comment '批次号',
    virtual_goods_id string comment '虚拟商品ID',
    project          string comment '站点',
    country          string comment '国家',
    platform_type    string comment '平台',
    event            string comment '事件类型',
    session_id       string comment 'session id'
) COMMENT '打版批次明细事实表'
    partitioned by (`pt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS orc
    TBLPROPERTIES ("orc.compress"="SNAPPY");