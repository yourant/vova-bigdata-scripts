-- 开启 mini-batch
SET table.exec.mini-batch.enabled=true;
-- mini-batch的时间间隔，即作业需要额外忍受的延迟
SET table.exec.mini-batch.allow-latency=1s;
-- 一个 mini-batch 中允许最多缓存的数据
SET table.exec.mini-batch.size=5;
-- 开启 local-global 优化
SET table.optimizer.agg-phase-strategy=TWO_PHASE;
-- 开启 distinct agg 切分
SET table.optimizer.distinct-agg.split.enabled=true;



CREATE TABLE vova_fact_log_goods_click(
  device_id STRING,
  collector_ts STRING,
  c1_ts AS TO_TIMESTAMP(REGEXP_REPLACE(collector_ts,'T|Z',''),'yyyy-MM-ddHH:mm:ss.SSS'),
  collector_tstamp bigint,
  c_ts AS TO_TIMESTAMP(FROM_UNIXTIME(collector_tstamp / 1000, 'yyyy-MM-ddHH:mm:ss'), 'yyyy-MM-ddHH:mm:ss'),
  proctime AS PROCTIME()
) WITH (
 'connector' = 'kafka',
 'topic' = 'vova_fact_log_goods_click',
 'properties.bootstrap.servers' = 'b-3.vova-bd-kafka-prod.ucyz9y.c2.kafka.us-east-1.amazonaws.com:9092,b-2.vova-bd-kafka-prod.ucyz9y.c2.kafka.us-east-1.amazonaws.com:9092,b-1.vova-bd-kafka-prod.ucyz9y.c2.kafka.us-east-1.amazonaws.com:9092',
 'properties.group.id' = 'vova_fact_log_goods_click_analysis',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true',
 'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE vova_fact_log_goods_impression(
  device_id STRING,
  collector_ts STRING,
  c1_ts AS TO_TIMESTAMP(REGEXP_REPLACE(collector_ts,'T|Z',''),'yyyy-MM-ddHH:mm:ss.SSS'),
  collector_tstamp bigint,
  c_ts AS TO_TIMESTAMP(FROM_UNIXTIME(collector_tstamp / 1000, 'yyyy-MM-ddHH:mm:ss'), 'yyyy-MM-ddHH:mm:ss'),
  proctime AS PROCTIME()
) WITH (
 'connector' = 'kafka',
 'topic' = 'vova_fact_log_goods_impression',
 'properties.bootstrap.servers' = 'b-3.vova-bd-kafka-prod.ucyz9y.c2.kafka.us-east-1.amazonaws.com:9092,b-2.vova-bd-kafka-prod.ucyz9y.c2.kafka.us-east-1.amazonaws.com:9092,b-1.vova-bd-kafka-prod.ucyz9y.c2.kafka.us-east-1.amazonaws.com:9092',
 'properties.group.id' = 'vova_fact_log_goods_impression_analysis',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true',
 'scan.startup.mode' = 'latest-offset'
);


CREATE TABLE ads_goods_clk_expre_analysis (
     cur_date varchar(20),
     cur_hour VARCHAR(5),
     cur_minute VARCHAR(5),
     expre_cnt BIGINT,
     clk_cnt   BIGINT,
     expre_uv  BIGINT
) WITH (
'connector.type'='jdbc',
'connector.url'='jdbc:mysql://rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/onequery?charset=utf8',
'connector.table'='ads_goods_clk_expre_analysis',
'connector.username'='wtbialas',
'connector.password'='7owTlL9d9dL7r07DDttvN72rJ2CP0VGJ',
'connector.write.flush.max-rows'='-1',
'connector.write.flush.interval' = '0s'
);


insert into ads_goods_clk_expre_analysis


select
DATE_FORMAT(c_ts,'yyyy-MM-dd') as cur_date,
DATE_FORMAT(current_timestamp, 'HH') as cur_hour,
DATE_FORMAT(current_timestamp, 'mm') as cur_minute,
count(*) filter(WHERE type = 'imp') as expre_cnt,
count(*) filter(WHERE type = 'clk') as clk_cnt,
count(distinct device_id) filter(WHERE type = 'imp') as expre_uv
from
(select
'clk' as type,
device_id,
c_ts,
proctime
from
vova_fact_log_goods_click
union all
select
'imp' as type,
device_id,
c_ts,
proctime
from
vova_fact_log_goods_impression)
where DATE_FORMAT(proctime,'yyyy-MM-dd') = DATE_FORMAT(c_ts,'yyyy-MM-dd')
GROUP BY  DATE_FORMAT(c_ts,'yyyy-MM-dd')
;