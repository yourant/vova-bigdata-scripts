-- 用户画像子品类偏好统计表
drop table if exists ads.ads_vova_buyer_portrait_feature;
CREATE TABLE `ads.ads_vova_buyer_portrait_feature`(
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
  `avg_price` decimal(13,2) COMMENT 'i_',
  `email_act` int COMMENT 'i_邮箱活跃度',
  `gmv_stage` int COMMENT 'd_gmv分层级别',
  `is_brand` int COMMENT 'd_是否brand爱好者'
)  COMMENT '用户画像子品类偏好统计表' PARTITIONED BY (pt string)
     STORED AS PARQUETFILE;
alter table ads.ads_vova_buyer_portrait_feature add columns(`sub_new_buyers` int comment '1.首单在七日内次新人用户，0.其它') cascade;
alter table ads.ads_vova_buyer_portrait_feature add columns(`is_order_complete` int comment 'is_order_complete :1是，0否') cascade;



CREATE TABLE ods_vova_vtp.ods_vova_newsletter_send_email_his(
  `nse_id` bigint COMMENT '',
  `nl_code` string COMMENT '',
  `nl_type` string COMMENT 'nl_type',
  `email` string COMMENT '用户email',
  `send_time` timestamp COMMENT '发送时间',
  `http_code` string COMMENT 'email发送是否成功的http code',
  `open_count` bigint COMMENT '统计用户打开的次数',
  `open_time` timestamp COMMENT '打开时间') stored as parquetfile;


CREATE TABLE ads.ads_vova_buyer_gmv_stage_3m (
  buyer_id int  COMMENT '用户id',
  country  String COMMENT '国家',
  gmv_stage int COMMENT '国家近三月客单价分层，1:小于1倍客单价，2：大于等于1倍客单价小于2倍客单价，3：大于等于2倍客单价小于等于3倍客单价，4：大于等于3倍客单价'
)comment '用户近三月消费区间评级' STORED AS PARQUETFILE;


用户近半年brand属性标签表
CREATE TABLE ads.ads_vova_buyer_brand_level(
  buyer_id int  COMMENT '用户id',
  is_brand int  COMMENT 'is_brand :1是brand，0是非brand'
)comment '用户近半年brand属性标签表' STORED AS PARQUETFILE;


是否完成过全平台体验属性标签表
CREATE TABLE ads.ads_vova_buyer_order_complate(
  buyer_id int  COMMENT '用户id',
  is_order_complete int  COMMENT 'is_order_complete :1是，0否'
)comment '是否完成过全平台体验属性标签表' STORED AS PARQUETFILE;

