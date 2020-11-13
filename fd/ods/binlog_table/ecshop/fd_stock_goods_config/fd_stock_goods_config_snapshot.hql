CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config (
    id bigint comment '自增id',
	goods_id bigint comment '外部商品id',
	min_quantity decimal(10,2) comment '起订量(件)',
	produce_days decimal(10,2) comment '生产天数',
	change_provider string comment '更换供应商 N:否 Y:是',
	change_provider_days decimal(10, 2) comment '更换供应商天数',
	change_provider_reason string comment '更换供应商原因',
	is_delete int comment '是否删除',
	update_date bigint comment '',
	fabric string comment '面料类型:A类-工厂-梭织100米,B类-工厂-针织70米,C类-工厂-呢料60米,D类-工厂-数码印花30米,F类-贸易-其他',
	provider_type string comment '生产类型:A-blouse-工厂：默认7天,B1-dress（素色）-工厂：默认7天,B2-dress（特殊工艺）-工厂：默认10天,C1-coats（无里布）/sweater-工厂：默认10天,C2-Coats（有里布）-工厂：默认12天,C3-Coats（棉服皮草）-工厂：默认15天,D1-Swimwear/shoes-贸易：默认10天,E-时装-贸易：默认7天,F：手填天数'
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config
select `(dt)?+.+` from ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config_arc where dt = '${hiveconf:dt}';
