CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config_arc (
    id bigint comment '自增id',
	goods_id bigint comment '外部商品id',
	min_quantity decimal(15, 4) comment '起订量(件)',
	produce_days decimal(15, 4) comment '生产天数',
	change_provider string comment '更换供应商 N:否 Y:是',
	change_provider_days decimal(15, 4) comment '更换供应商天数',
	change_provider_reason string comment '更换供应商原因',
	is_delete bigint comment '是否删除',
	update_date bigint comment '',
	fabric string comment '面料类型:A类-工厂-梭织100米,B类-工厂-针织70米,C类-工厂-呢料60米,D类-工厂-数码印花30米,F类-贸易-其他',
	provider_type string comment '生产类型:A-blouse-工厂：默认7天,B1-dress（素色）-工厂：默认7天,B2-dress（特殊工艺）-工厂：默认10天,C1-coats（无里布）/sweater-工厂：默认10天,C2-Coats（有里布）-工厂：默认12天,C3-Coats（棉服皮草）-工厂：默认15天,D1-Swimwear/shoes-贸易：默认10天,E-时装-贸易：默认7天,F：手填天数'
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config_arc PARTITION (dt = '${hiveconf:dt}')
select 
     id,goods_id,min_quantity,produce_days,change_provider,change_provider_days,change_provider_reason,is_delete,update_date,fabric,provider_type
from (

    select 
        dt,id,goods_id,min_quantity,produce_days,change_provider,change_provider_days,change_provider_reason,is_delete,update_date,fabric,provider_type,
        row_number () OVER (PARTITION BY id ORDER BY dt DESC) AS rank
    from (

        select  dt
                id,
                goods_id,
                min_quantity,
                produce_days,
                change_provider,
                change_provider_days,
                change_provider_reason,
                is_delete,
                update_date,
                fabric,
                provider_type
        from ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config_arc where dt = '${hiveconf:dt_last}'

        UNION

        select dt,id,goods_id,min_quantity,produce_days,change_provider,change_provider_days,change_provider_reason,is_delete,update_date,fabric,provider_type
        from (

            select  dt
                    id,
                    goods_id,
                    min_quantity,
                    produce_days,
                    change_provider,
                    change_provider_days,
                    change_provider_reason,
                    is_delete,
                    update_date,
                    fabric,
                    provider_type,
                    row_number () OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_ecshop.ods_fd_ecs_fd_stock_goods_config_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
