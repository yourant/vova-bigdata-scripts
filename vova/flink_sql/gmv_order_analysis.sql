-- 开启 mini-batch
SET table.exec.mini-batch.enabled=false ;
-- mini-batch的时间间隔，即作业需要额外忍受的延迟
SET table.exec.mini-batch.allow-latency=1s;
-- 一个 mini-batch 中允许最多缓存的数据
SET table.exec.mini-batch.size=1;
-- 开启 local-global 优化
SET table.optimizer.agg-phase-strategy=TWO_PHASE;
-- 开启 distinct agg 切分
SET table.optimizer.distinct-agg.split.enabled=true;


CREATE TABLE fact_pay(
  datasource VARCHAR,
  region_code VARCHAR,
  goods_number BIGINT,
  shop_price DECIMAL,
  shipping_fee DECIMAL,
  pay_time VARCHAR,
  pay_ts AS TO_TIMESTAMP(REGEXP_REPLACE(pay_time,'T|Z',''),'yyyy-MM-ddHH:mm:ss.SSS'),
  order_goods_id BIGINT,
  order_id BIGINT,
  from_domain VARCHAR,
  og_ts BIGINT,
  pay_status BIGINT,
  email VARCHAR,
  parent_order_id BIGINT,
  buyer_id BIGINT,
  proctime AS PROCTIME()
) WITH (
 'connector' = 'kafka',
 'topic' = 'dwd_fact_pay_v2',
 'properties.bootstrap.servers' = 'b-3.vova-bd-kafka-prod.ucyz9y.c2.kafka.us-east-1.amazonaws.com:9092,b-2.vova-bd-kafka-prod.ucyz9y.c2.kafka.us-east-1.amazonaws.com:9092,b-1.vova-bd-kafka-prod.ucyz9y.c2.kafka.us-east-1.amazonaws.com:9092',
 'properties.group.id' = 'ads_gmv_uv_analysis2',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true',
 'scan.startup.mode' = 'latest-offset'
);


CREATE TABLE ads_gmv_order_analysis (
     cur_date varchar(20),
     cur_hour VARCHAR(5),
     cur_minute VARCHAR(5),
     gmv DECIMAL(10,2),
     order_cnt BIGINT,
     pay_uv BIGINT
) WITH (
'connector.type'='jdbc',
'connector.url'='jdbc:mysql://rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/onequery?charset=utf8',
'connector.table'='ads_gmv_order_analysis',
'connector.username'='wtbialas',
'connector.password'='7owTlL9d9dL7r07DDttvN72rJ2CP0VGJ',
'connector.write.flush.max-rows'='1',
'connector.write.flush.interval' = '0s'
);

insert into ads_gmv_order_analysis
select
 DATE_FORMAT(pay_ts,'yyyy-MM-dd') as cur_date,
DATE_FORMAT(current_timestamp, 'HH') as cur_hour,
DATE_FORMAT(current_timestamp, 'mm') as cur_minute,
sum(gmv) as gmv,
count(distinct order_id) as order_cnt,
count(distinct buyer_id) as pay_uv
from
(
select
order_id,
buyer_id,
pay_ts,
COALESCE(shop_price*goods_number+shipping_fee,0) as gmv,
pay_time,
ROW_NUMBER() OVER (PARTITION BY order_goods_id ORDER BY og_ts DESC) AS rownum
from fact_pay
where
pay_status >= 1 and DATE_FORMAT(proctime,'yyyy-MM-dd') = DATE_FORMAT(pay_ts,'yyyy-MM-dd')
and NOT REGEXP(email, '@airydress.com|@tetx.com|@qq.com|@i9i8.com')
and parent_order_id = 0
) t0 where t0.rownum=1
group by  DATE_FORMAT(pay_ts,'yyyy-MM-dd')
;