CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config_inc (
    `id` bigint COMMENT 'id',
    `goods_id` bigint COMMENT '外部商品id',
    `min_quantity` decimal(13,4) COMMENT '起订量(件)',
    `produce_days` decimal(13,4) COMMENT '生产天数',
    `change_provider` string COMMENT '更换供应商 N:否 Y:是',
    `change_provider_days` decimal(13,4) COMMENT '更换供应商天数',
    `change_provider_reason` string COMMENT '更换供应商原因',
    `pms_purchase` string COMMENT '是否采购面辅料（Y是；N否）',
    `pms_purchase_days` bigint COMMENT '面辅料采购天数',
    `is_delete` bigint COMMENT '是否删除',
    `update_date` timestamp COMMENT '',
    `fabric` string COMMENT '面料类型:A类-工厂-梭织100米,B类-工厂-针织70米,C类-工厂-呢料60米,D类-工厂-数码印花30米,F类-贸易-其他',
    `provider_type` string COMMENT '生产类型:A-blouse-工厂：默认7天,B1-dress（素色）-工厂：默认7天,B2-dress（特殊工艺）-工厂：默认10天,C1-coats（无里布）/sweater-工厂：默认10天,C2-Coats（有里布）-工厂：默认12天,C3-Coats（棉服皮草）-工厂：默认15天,D1-Swimwear/shoes-贸易：默认10天,E-时装-贸易：默认7天,F：手填天数',
    `audit_submit_time` timestamp COMMENT '待审核提交时间',
    `audit_action_id` bigint COMMENT '改变状态为待审核时的action_id，见表fd_stock_goods_config_action的id字段',
    `status` bigint COMMENT '状态：0默认（不需审核），1审核通过，2审核失败，3待审核'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config_arc (
    `id` bigint COMMENT 'id',
    `goods_id` bigint COMMENT '外部商品id',
    `min_quantity` decimal(13,4) COMMENT '起订量(件)',
    `produce_days` decimal(13,4) COMMENT '生产天数',
    `change_provider` string COMMENT '更换供应商 N:否 Y:是',
    `change_provider_days` decimal(13,4) COMMENT '更换供应商天数',
    `change_provider_reason` string COMMENT '更换供应商原因',
    `pms_purchase` string COMMENT '是否采购面辅料（Y是；N否）',
    `pms_purchase_days` bigint COMMENT '面辅料采购天数',
    `is_delete` bigint COMMENT '是否删除',
    `update_date` timestamp COMMENT '',
    `fabric` string COMMENT '面料类型:A类-工厂-梭织100米,B类-工厂-针织70米,C类-工厂-呢料60米,D类-工厂-数码印花30米,F类-贸易-其他',
    `provider_type` string COMMENT '生产类型:A-blouse-工厂：默认7天,B1-dress（素色）-工厂：默认7天,B2-dress（特殊工艺）-工厂：默认10天,C1-coats（无里布）/sweater-工厂：默认10天,C2-Coats（有里布）-工厂：默认12天,C3-Coats（棉服皮草）-工厂：默认15天,D1-Swimwear/shoes-贸易：默认10天,E-时装-贸易：默认7天,F：手填天数',
    `audit_submit_time` timestamp COMMENT '待审核提交时间',
    `audit_action_id` bigint COMMENT '改变状态为待审核时的action_id，见表fd_stock_goods_config_action的id字段',
    `status` bigint COMMENT '状态：0默认（不需审核），1审核通过，2审核失败，3待审核'
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;


CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config (
    `id` bigint COMMENT 'id',
    `goods_id` bigint COMMENT '外部商品id',
    `min_quantity` decimal(13,4) COMMENT '起订量(件)',
    `produce_days` decimal(13,4) COMMENT '生产天数',
    `change_provider` string COMMENT '更换供应商 N:否 Y:是',
    `change_provider_days` decimal(13,4) COMMENT '更换供应商天数',
    `change_provider_reason` string COMMENT '更换供应商原因',
    `pms_purchase` string COMMENT '是否采购面辅料（Y是；N否）',
    `pms_purchase_days` bigint COMMENT '面辅料采购天数',
    `is_delete` bigint COMMENT '是否删除',
    `update_date` timestamp COMMENT '',
    `fabric` string COMMENT '面料类型:A类-工厂-梭织100米,B类-工厂-针织70米,C类-工厂-呢料60米,D类-工厂-数码印花30米,F类-贸易-其他',
    `provider_type` string COMMENT '生产类型:A-blouse-工厂：默认7天,B1-dress（素色）-工厂：默认7天,B2-dress（特殊工艺）-工厂：默认10天,C1-coats（无里布）/sweater-工厂：默认10天,C2-Coats（有里布）-工厂：默认12天,C3-Coats（棉服皮草）-工厂：默认15天,D1-Swimwear/shoes-贸易：默认10天,E-时装-贸易：默认7天,F：手填天数',
    `audit_submit_time` timestamp COMMENT '待审核提交时间',
    `audit_action_id` bigint COMMENT '改变状态为待审核时的action_id，见表fd_stock_goods_config_action的id字段',
    `status` bigint COMMENT '状态：0默认（不需审核），1审核通过，2审核失败，3待审核'
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

