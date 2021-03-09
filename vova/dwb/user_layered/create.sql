DROP TABLE IF EXISTS dwb.dwb_vova_user_layered_result;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_user_layered_result
(
now_date string, --日期
dau string, --dau
latent_user string,  --潜在用户
latent_user_rate string,
to_change_user string,  --待转化用户
to_change_user_rate string,
new_user string,  --新用户
new_user_rate string,
active_user string,  --活跃用户
active_user_rate string,
first_order_user string, --首单用户
first_order_user_rate string,
loyal_user string, --忠诚用户
loyal_user_rate string,
lowBuy_highActive_user string, --低复购高活跃
lowBuy_highActive_user_rate string,
lowBuy_lowActive_user string, --低复购低活跃
lowBuy_lowActive_user_rate string,
silent string, --沉默用户
silent_rate string,
leave_user string, --流失用户
leave_rate string
) COMMENT '用户分层' PARTITIONED BY (pt STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

alter table dwb.dwb_vova_user_layered_result ADD COLUMNS (
platform STRING COMMENT '平台',
region_code STRING COMMENT '国家'
) CASCADE;

CREATE TABLE dwb_vova_user_layered_result(
now_date CHAR(30),
dau CHAR(30),
latent_user CHAR(30),
latent_user_rate CHAR(30),
to_change_user CHAR(30),
to_change_user_rate CHAR(30),
new_user CHAR(30),
new_user_rate CHAR(30),
active_user CHAR(30),
active_user_rate CHAR(30),
first_order_user CHAR(30),
first_order_user_rate CHAR(30),
loyal_user CHAR(30),
loyal_user_rate CHAR(30),
lowBuy_highActive_user CHAR(30),
lowBuy_highActive_user_rate CHAR(30),
lowBuy_lowActive_user CHAR(30),
lowBuy_lowActive_user_rate CHAR(30),
silent CHAR(30),
silent_rate CHAR(30),
leave_user CHAR(30),
leave_rate CHAR(30)
);