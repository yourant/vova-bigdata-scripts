CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_inventory_summary_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    inventory_summary_id  string comment '主键',
    status_id             string comment '仓库类型',
    facility_id           string comment '仓库id',
    container_id          string comment '容器id',
    product_id            string comment '产品id',
    stock_quantity        decimal(15, 4) comment '库存量',
    available_to_reserved decimal(15, 4) comment '可预定量',
    demand_quantity       decimal(15, 4) COMMENT '订单需求量',
    making_quantity       decimal(15, 4) COMMENT '',
    created_stamp         bigint comment '创建时间',
    last_updated_stamp    bigint comment '修改时间',
    last_updated_tx_stamp bigint comment '修改事务时间',
    created_tx_stamp      bigint comment '创建事务时间',
    comments              string comment '备注',
    currency_uom_id       string comment '量词',
    uom_id                string comment '量词',
    owner_party_id        string comment '用户组织id',
    party_id              string comment '商品组织id',
    unit_cost             decimal(15, 4) comment '商品单价'
) comment ''
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_inventory_summary_arc (
    inventory_summary_id  string comment '主键',
    status_id             string comment '仓库类型',
    facility_id           string comment '仓库id',
    container_id          string comment '容器id',
    product_id            string comment '产品id',
    stock_quantity        decimal(15, 4) comment '库存量',
    available_to_reserved decimal(15, 4) comment '可预定量',
    demand_quantity       decimal(15, 4) COMMENT '订单需求量',
    making_quantity       decimal(15, 4) COMMENT '',
    created_stamp         bigint comment '创建时间',
    last_updated_stamp    bigint comment '修改时间',
    last_updated_tx_stamp bigint comment '修改事务时间',
    created_tx_stamp      bigint comment '创建事务时间',
    comments              string comment '备注',
    currency_uom_id       string comment '量词',
    uom_id                string comment '量词',
    owner_party_id        string comment '用户组织id',
    party_id              string comment '商品组织id',
    unit_cost             decimal(15, 4) comment '商品单价'
) comment ''
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;


CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_inventory_summary (
    inventory_summary_id  string comment '主键',
    status_id             string comment '仓库类型',
    facility_id           string comment '仓库id',
    container_id          string comment '容器id',
    product_id            string comment '产品id',
    stock_quantity        decimal(15, 4) comment '库存量',
    available_to_reserved decimal(15, 4) comment '可预定量',
    demand_quantity       decimal(15, 4) COMMENT '订单需求量',
    making_quantity       decimal(15, 4) COMMENT '',
    created_stamp         bigint comment '创建时间',
    last_updated_stamp    bigint comment '修改时间',
    last_updated_tx_stamp bigint comment '修改事务时间',
    created_tx_stamp      bigint comment '创建事务时间',
    comments              string comment '备注',
    currency_uom_id       string comment '量词',
    uom_id                string comment '量词',
    owner_party_id        string comment '用户组织id',
    party_id              string comment '商品组织id',
    unit_cost             decimal(15, 4) comment '商品单价'
) comment ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

