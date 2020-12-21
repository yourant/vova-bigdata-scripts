--商户相关记录明细表,商户登录，商户上下架商品，商户禁售等
drop table if exists dwd.dwd_vova_fact_mbrmct_mct_cd;
create table dwd.dwd_vova_fact_mbrmct_mct_cd
(
    `datasource` string COMMENT '数据源',
    `mct_id`     bigint COMMENT '商户id',
    `id`         string COMMENT '数据类型1：ip，2：md5号，3：goods_id',
    `start_dt`   timestamp COMMENT '开始时间',
    `end_dt`     timestamp COMMENT '结束时间',
    `cnt_cd`     bigint COMMENT '当天发生次数',
    `cnt_td`     bigint COMMENT '截止当天发生次数',
    `status`     bigint COMMENT '状态,-1:无状态,其它待定一次',
    `act_type`   bigint COMMENT '1:登录ip，2:md5'
) COMMENT '每日'
    PARTITIONED BY ( `pt` string)
    stored as parquet;