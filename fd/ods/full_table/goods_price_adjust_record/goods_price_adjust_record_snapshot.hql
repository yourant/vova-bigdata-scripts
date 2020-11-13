CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_goods_price_adjust_record
(
  id bigint
    ,gppar_id bigint COMMENT '核价申请表ID'
    ,goods_id bigint COMMENT '商品ID'
    ,virtual_goods_id bigint COMMENT '商品虚拟ID'
    ,project string COMMENT '商品所属组织'
    ,adjust_type bigint COMMENT '商品调价类型 0:未定义,1:直接调价,2:核价后调价'
    ,worker string COMMENT '提交人'
    ,status bigint COMMENT '当前状态 0:待审,1:撤销,2:通过,3:驳回'
    ,old_market_price decimal(15, 4)  COMMENT 'old市场价'
    ,new_market_price decimal(15, 4)  COMMENT 'new市场价'
    ,old_shop_price decimal(15, 4)  COMMENT 'old销售价'
    ,new_shop_price decimal(15, 4)  COMMENT 'new销售价'
    ,old_purchase_price decimal(15, 4)  COMMENT 'old采购价'
    ,new_purchase_price decimal(15, 4)  COMMENT 'new采购价'
    ,old_times_rate decimal(15, 4)  COMMENT 'old倍率'
    ,new_times_rate decimal(15, 4)  COMMENT 'new倍率'
    ,created bigint COMMENT '创建时间'
    ,modified bigint COMMENT '修改时间'
 )comment '申请调价核价操作历史表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_goods_price_adjust_record
select `(dt)?+.+` from ods_fd_vb.ods_fd_goods_price_adjust_record_arc where dt = '${hiveconf:dt}';
