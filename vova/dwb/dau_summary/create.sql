DROP TABLE IF EXISTS dwb.dwb_vova_user_layered_result;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_user_layered_result
(
now_date string COMMENT 'i_日期',
dau string COMMENT 'i_dau',
latent_user string COMMENT 'i_潜在用户',
latent_user_rate string COMMENT 'i_潜在用户',
to_change_user string COMMENT 'i_待转化用户', 
to_change_user_rate string COMMENT 'i_to_change_user_rate',
new_user string COMMENT 'i_新用户', 
new_user_rate string COMMENT 'i_new_user_rate',
active_user string COMMENT 'i_活跃用户', 
active_user_rate string COMMENT 'i_active_user_rate',
first_order_user string COMMENT 'i_首单用户', 
first_order_user_rate string COMMENT 'i_first_order_user_rate',
loyal_user string COMMENT 'i_忠诚用户', 
loyal_user_rate string COMMENT 'i_loyal_user_rate',
lowBuy_highActive_user string COMMENT 'i_低复购高活跃',
lowBuy_highActive_user_rate string COMMENT 'i_lowBuy_highActive_user_rate',
lowBuy_lowActive_user string COMMENT 'i_低复购低活跃', 
lowBuy_lowActive_user_rate string COMMENT 'i_lowBuy_lowActive_user_rate',
silent string COMMENT 'i_沉默用户',
silent_rate string COMMENT 'i_silent_rate',
leave_user string COMMENT 'i_流失用户', 
leave_rate string COMMENT 'i_leave_rate'
) COMMENT '用户分层' PARTITIONED BY (pt STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

