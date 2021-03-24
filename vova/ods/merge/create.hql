CREATE EXTERNAL TABLE `ods_vova_vts.ods_vova_user_mission_record`(
  `umr_id` bigint COMMENT '',
  `user_id` bigint COMMENT '',
  `mission_id` bigint COMMENT 'missions表主键',
  `mission_reward_type` string COMMENT '奖励类型',
  `mission_reward_value` string COMMENT '任务奖励',
  `is_complete` bigint COMMENT '是否完成',
  `is_receive_reward` bigint COMMENT '是否领取奖励',
  `from_domain` string COMMENT '完成的域名',
  `platform` string COMMENT '完成平台',
  `app_version` string COMMENT 'APP版本',
  `device_id` string COMMENT 'APP设备号',
  `idfa` string COMMENT 'ios idfa',
  `idfv` string COMMENT 'ios idfv',
  `organic_idfv` string COMMENT 'ios organic idfv',
  `imei` string COMMENT 'android imei',
  `android_id` string COMMENT 'android android_id',
  `create_time` timestamp COMMENT '',
  `last_update_time` timestamp COMMENT '',
  `timezone` string COMMENT '') COMMENT 'ods_vova_user_mission_record merge table
  STORED AS PARQUETFILE;



CREATE EXTERNAL TABLE `ods_vova_vts.ods_vova_user_wallet_ops_log`(
  `uwol_id` bigint COMMENT '',
  `user_wallet_id` bigint COMMENT '',
  `user_id` bigint COMMENT '',
  `ops_field` string COMMENT '对user_wallet_part表进行操作的字段名',
  `ops` string COMMENT '积分操作(obtain:获取 deduct:减少 exchange:兑换 return:撤销兑换) 分销收益操作(earnings_friend_consume:好友已消费 earnings_friend_refund:好友已退货 earnings_buy:使用零钱购买商品 earnings_merchant_cancel:商家取消交易/商品退款 earnings_withdraw:提现 earnings_withdraw_success:提现成功 earnings_withdraw_fail:提现失败)',
  `old_value` decimal(13,4) COMMENT '操作前的field值',
  `value` decimal(13,4) COMMENT '操作后的field值',
  `exchange_type` string COMMENT '兑换的类型，coupon',
  `exchange_value` string COMMENT '兑换的值，coupon_code',
  `pk_tname` string COMMENT 'log明细关联的表名',
  `pk_id` bigint COMMENT 'log明细关联的表的主键值',
  `version` bigint COMMENT '区分不同规则下产生的ops',
  `from_domain` string COMMENT '完成的域名',
  `platform` string COMMENT '完成平台',
  `app_version` string COMMENT 'APP版本',
  `device_id` string COMMENT 'APP设备号',
  `idfa` string COMMENT 'ios idfa',
  `idfv` string COMMENT 'ios idfv',
  `organic_idfv` string COMMENT 'ios organic idfv',
  `imei` string COMMENT 'android imei',
  `android_id` string COMMENT 'android android_id',
  `worker` string COMMENT '操作人',
  `comment` string COMMENT '备注',
  `create_time` timestamp COMMENT '',
  `last_update_time` timestamp COMMENT '') COMMENT 'ods_vova_user_wallet_ops_log merge table'
STORED AS PARQUETFILE;


CREATE EXTERNAL TABLE `ods_vova_vts.ods_vova_user_wallet_part`(
  `uw_id` bigint COMMENT '',
  `user_id` bigint COMMENT '用户id',
  `coins` bigint COMMENT 'Coins数量',
  `balance` decimal(13,4) COMMENT '钱包余额',
  `lock_balance` decimal(13,4) COMMENT '不可用余额',
  `is_balance_unlock` bigint COMMENT '钱包零钱是否解锁，0未解锁，1解锁',
  `distribution_config_id` bigint COMMENT '用户分销相关配置',
  `dog_gold` decimal(13,4) COMMENT '幸运狗活动金币',
  `dog_remaining_cash` decimal(13,4) COMMENT '幸运狗剩余可直接提现金额',
  `wallet` decimal(13,4) COMMENT '通用钱包余额，与前面的钱包不是一个东西',
  `freebies_remaining_cash` decimal(13,4) COMMENT '0元购剩余可直接提现金额',
  `dig_crystal_cash` decimal(13,4) COMMENT '挖矿现金',
  `create_time` timestamp COMMENT '',
  `last_update_time` timestamp COMMENT '') COMMENT 'ods_vova_user_wallet_part merge table'
STORED AS PARQUETFILE;


CREATE EXTERNAL TABLE `ods_vova_vrf.ods_vova_goods_languages_merge`(
  `id` bigint COMMENT '自增ID',
  `goods_id` bigint COMMENT '商品Id',
  `language_code` string COMMENT '语言',
  `source` string COMMENT '翻译来源',
  `goods_name` string COMMENT '商品名称',
  `keywords` string COMMENT '商品关键字',
  `goods_desc` string COMMENT '商品描述',
  `create_time` timestamp COMMENT '添加时间',
  `last_update_time` timestamp COMMENT '最后更新时间'
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


