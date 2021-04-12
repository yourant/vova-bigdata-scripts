drop table ads.ads_vova_buyer_portrait_d;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_buyer_portrait_d
(
    user_id                bigint COMMENT '用户id',
    pay_cnt_his            bigint COMMENT '历史支付成功订单数',
    ship_cnt_his           bigint COMMENT '历史发货成功订单数',
    max_visits_cnt_cw      bigint COMMENT '过去的每个自然周访问的最高频次，0~7',
    price_range            string COMMENT '价格区间',
    gmv_stage              int comment '国家近三月客单价分层，1:小于1倍客单价，2：大于等于1倍客单价小于2倍客单价，3：大于等于2倍客单价小于等于3倍客单价，4：大于等于3倍客单价,0:默认值'
) COMMENT ''
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;



 CREATE TABLE ads_buyer_portrait_d (
  id int(10) NOT NULL AUTO_INCREMENT,
  user_id bigint(20)  NOT NULL COMMENT '用户id',
  pay_cnt_his bigint(20) NOT NULL DEFAULT '0' COMMENT '历史支付成功订单数',
  ship_cnt_his bigint(20) NOT NULL DEFAULT '0' COMMENT '历史发货成功订单数',
  max_visits_cnt_cw bigint(20) NOT NULL DEFAULT '0' COMMENT '过去的每个自然周访问的最高频次，0-7',
  price_range varchar(32) DEFAULT '' COMMENT '价格区间'
  PRIMARY KEY (user_id)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='用户画像';

drop table ads.ads_vova_buyer_push_portrait;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_buyer_push_portrait
(
    user_id                bigint COMMENT '用户id',
    email                  string COMMENT '邮箱',
    gender                 string COMMENT '性别',
    region_code            string COMMENT '国家',
    language               string COMMENT '语言',
    user_age_group         string COMMENT '年龄段',
    reg_tag                string COMMENT '注册时长',
    buyer_act              string COMMENT '用户活跃度',
    trade_act              string COMMENT '交易阶段',
    price_prefer           string COMMENT '价格偏好',
    goods_id_a             bigint COMMENT '用户偏好商品A',
    goods_name_a           string COMMENT '用户偏好商品A对应语言的标题',
    goods_thumb_a          string COMMENT '用户偏好商品A对应语言的主图url',
    goods_keywords_a       string COMMENT '用户偏好商品A对应语言的关键词',
    goods_id_b             bigint COMMENT '用户偏好商品B',
    goods_name_b           string COMMENT '用户偏好商品B对应语言的标题',
    goods_thumb_b          string COMMENT '用户偏好商品B对应语言的主图url',
    goods_keywords_b       string COMMENT '用户偏好商品B对应语言的关键词',
    last_logint_type       bigint COMMENT '上次登入间隔类型',
    last_buyer_type        bigint COMMENT '上次购买间隔类型',
    buy_times_type         bigint COMMENT '近90天购买频率',
    utc                    bigint COMMENT '时区',
    email_act              bigint COMMENT 'i_邮箱活跃度',
    is_brand               bigint COMMENT 'd_是否brand爱好者'
) COMMENT '推送用户画像'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

alter table ads.ads_vova_buyer_push_portrait add columns(`gmv_stage` int comment '国家近三月客单价分层，1:小于1倍客单价，2：大于等于1倍客单价小于2倍客单价，3：大于等于2倍客单价小于等于3倍客单价，4：大于等于3倍客单价') cascade;







 CREATE TABLE user_push_portrait (
  user_id int(11)  NOT NULL COMMENT '用户id',
  email varchar(60)  COMMENT '邮箱',
  gender varchar(16)  COMMENT '性别',
  region_code varchar(16)  COMMENT '国家',
  language varchar(16)  COMMENT '语言',
  user_age_group varchar(16)  COMMENT '年龄段',
  reg_tag varchar(16)  COMMENT '注册时长',
  buyer_act varchar(16)  COMMENT '用户活跃度',
  trade_act varchar(16)  COMMENT '交易阶段',
  price_prefer varchar(16)  COMMENT '价格偏好',
  goods_id_a int(11)  COMMENT '用户偏好商品A',
  goods_name_a varchar(255) COMMENT '用户偏好商品A对应语言的标题',
  goods_thumb_a varchar(255)  COMMENT '用户偏好商品A对应语言的主图url',
  goods_keywords_a varchar(255)  COMMENT '用户偏好商品A对应语言的关键词',
  goods_id_b int(11)  COMMENT '用户偏好商品B',
  goods_name_b varchar(255)  COMMENT '用户偏好商品B对应语言的标题',
  goods_thumb_b varchar(255) COMMENT '用户偏好商品B对应语言的主图url',
  goods_keywords_b varchar(255)  COMMENT '用户偏好商品B对应语言的关键词',
  PRIMARY KEY (user_id),
  KEY region_code (region_code) USING BTREE,
  KEY language (language) USING BTREE,
  KEY user_age_group (user_age_group) USING BTREE,
  KEY reg_tag (reg_tag) USING BTREE,
  KEY buyer_act (buyer_act) USING BTREE,
  KEY trade_act (trade_act) USING BTREE,
  KEY price_prefer (price_prefer) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='推送用户画像';


