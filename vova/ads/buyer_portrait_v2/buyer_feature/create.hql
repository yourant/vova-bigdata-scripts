-- 用户画像子品类偏好统计表
drop table if exists ads.ads_vova_buyer_portrait_feature;
CREATE external TABLE ads.ads_vova_buyer_portrait_feature(
  `buyer_id` bigint COMMENT 'd_买家id',
  `reg_gender` string COMMENT 'i_注册性别',
  `reg_age_group` string COMMENT 'i_注册年龄段',
  `reg_time` date COMMENT 'i_注册时间',
  `reg_ctry` string COMMENT 'i_注册国家',
  `lag_id` string COMMENT 'i_语言ID',
  `reg_channel` string COMMENT 'i_注册渠道',
  `os_type` string COMMENT 'i_手机系统',
  `gs_ids_valid_clk_1m` string COMMENT 'i_近30天有效点击goods_id集合（最多保留100个）',
  `gs_ids_collect_2m` string COMMENT '近60收藏goods_id集合',
  `gs_ids_add_cat_2m` string COMMENT '近60加购goods_id集合',
  `gs_ids_ord` string COMMENT '历史下单goods_id集合',
  `reg_tag` string COMMENT 'i_注册时间划分',
  `buyer_act` string COMMENT 'i_用户活跃度',
  `trade_act` string COMMENT 'i_交易阶段',
  `datasource` string COMMENT 'd_项目',
  `last_logint_type` bigint COMMENT '上次登入间隔类型',
  `last_buyer_type` bigint COMMENT '上次购买间隔类型',
  `buy_times_type` bigint COMMENT '近90天购买频率',
  `first_order_time` timestamp COMMENT 'i_首单时间',
  `order_cnt` int COMMENT 'i_购买订单数',
  `last_order_time` timestamp COMMENT 'i_最后一单时间',
  `avg_price` decimal(13,2) COMMENT 'i_笔单价',
  `email_act` int COMMENT 'i_邮箱活跃度',
  `gmv_stage` int COMMENT 'd_gmv分层级别',
  `is_brand` int COMMENT 'd_是否brand爱好者')
COMMENT '用户画像子品类偏好统计表' PARTITIONED BY (pt string)  STORED AS PARQUETFILE;

