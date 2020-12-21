-- 月卡用户状态更新
bootstrap-server: b-2.vova-messagequeue-hub.0k9k11.c2.kafka.us-east-1.amazonaws.com:9092,b-3.vova-messagequeue-hub.0k9k11.c2.kafka.us-east-1.amazonaws.com:9092,b-1.vova-messagequeue-hub.0k9k11.c2.kafka.us-east-1.amazonaws.com:9092
topic: vvqueue-bonus_card_status

-- 输出的 s3 路径
s3://bigdata-offline/warehouse/pdb/vova/vvqueue/vvqueue-bonus_card_status/pt=%Y-%m-%d

-- 文件格式:text 数据格式为:json
{"bonus_card_id":"43738","user_id":"101938828","old_status":"","new_status":"unpaid","update_time":1605595451}

-- ods 外部表
CREATE EXTERNAL TABLE ods.vova_bonus_card_status (
    data string
) COMMENT '月卡红包用户数据' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION 's3://bigdata-offline/warehouse/pdb/vova/vvqueue/vvqueue-bonus_card_status/';

msck repair table ods.vova_bonus_card_status;

-- dwd 解析后数据表
drop table dwd.dwd_vova_fact_log_bonus_card;
CREATE TABLE dwd.dwd_vova_fact_log_bonus_card (
  bonus_card_id bigint    COMMENT '月卡订单id',
  user_id       bigint    COMMENT '卖家id',
  old_status    string    COMMENT '原始状态status有4种"", "unpaid", "pending", "paid",新增条目时，old_status为空字符串',
  new_status    string    COMMENT '更新状态',
  update_time   bigint    COMMENT '更新时间'
) COMMENT '月卡红包用户状态变更日志'
PARTITIONED BY (pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


-- 如果ods 层数据同步异常，需要回滚 dwd.fact_log_bonus_card 数据，直接执行一下sql, 日期可自定义
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert OVERWRITE TABLE dwd.fact_log_bonus_card PARTITION (pt)
select
/*+ REPARTITION(1) */
  CAST(get_json_object(data, '$.bonus_card_id') AS BIGINT) bonus_card_id,
  CAST(get_json_object(data, '$.user_id') AS BIGINT) user_id,
  get_json_object(data, '$.old_status') old_status,
  get_json_object(data, '$.new_status') new_status,
  get_json_object(data, '$.update_time') update_time,
  from_unixtime(get_json_object(data, '$.update_time'), 'yyyy-MM-dd') pt
from
  ods.vova_bonus_card_status
where pt >= '2020-11-18' and pt <= '2020-11-19'
;
