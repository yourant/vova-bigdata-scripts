create  table if not exists dwd.dwd_fd_goods_event_detail
(
    goods_id                    bigint comment '商品ID',
    virtual_goods_id            bigint comment '虚拟商品ID',
    project_name                string comment '组织',
    country_code                string comment '国家',
    cat_name                     string comment '品类名',
    cat_id                    string comment '品类id',
    platform                    string comment '平台',
    device_type                 string comment '设备类型',
    session_id                  string comment '会话session id',
    event_name                 string comment '事件名称',
    page_code                  string comment '页面代码',
    mkt_source                  string comment '页面来源（广告否）',
    list_type                   string comment '列表类型',
    source_type                  string comment '平台分类'
)comment '商品明细表'
    partitioned by (`pt` string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;
