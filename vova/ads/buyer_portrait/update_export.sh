#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo $pre_date

sql="
drop table if exists themis.ads_buyer_portrait_d_new;
drop table if exists themis.ads_buyer_portrait_d_pre;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis.ads_buyer_portrait_d_new (
  user_id int  NOT NULL COMMENT '用户id',
  pay_cnt_his int NOT NULL DEFAULT '0' COMMENT '历史支付成功订单数',
  ship_cnt_his int NOT NULL DEFAULT '0' COMMENT '历史发货成功订单数',
  max_visits_cnt_cw int NOT NULL DEFAULT '0' COMMENT '过去的每个自然周访问的最高频次，0-7',
  price_range varchar(32) NOT NULL DEFAULT '' COMMENT '价格区间',
  gmv_stage int  NOT NULL DEFAULT '0' COMMENT '分国家近三月客单价分层，1:小于1倍客单价，2：大于等于1倍客单价小于2倍客单价，3：大于等于2倍客单价小于等于3倍客单价，4：大于等于3倍客单价,0:默认值',
  sub_new_buyers tinyint NOT NULL DEFAULT '0' COMMENT '1.首单在七日内次新人用户，0.其它',
  is_order_complete tinyint NOT NULL DEFAULT '0' COMMENT '1.完成过一次全平台订单体验，0.其它',
  PRIMARY KEY (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户画像';

CREATE TABLE IF NOT EXISTS themis.ads_buyer_portrait_d (
  user_id int  NOT NULL COMMENT '用户id',
  pay_cnt_his int NOT NULL DEFAULT '0' COMMENT '历史支付成功订单数',
  ship_cnt_his int NOT NULL DEFAULT '0' COMMENT '历史发货成功订单数',
  max_visits_cnt_cw int NOT NULL DEFAULT '0' COMMENT '过去的每个自然周访问的最高频次，0-7',
  price_range varchar(32) NOT NULL DEFAULT '' COMMENT '价格区间',
  gmv_stage int  NOT NULL DEFAULT '0' COMMENT '分国家近三月客单价分层，1:小于1倍客单价，2：大于等于1倍客单价小于2倍客单价，3：大于等于2倍客单价小于等于3倍客单价，4：大于等于3倍客单价,0:默认值',
  sub_new_buyers tinyint NOT NULL DEFAULT '0' COMMENT '1.首单在七日内次新人用户，0.其它',
  is_order_complete tinyint NOT NULL DEFAULT '0' COMMENT '1.完成过一次全平台订单体验，0.其它',
  PRIMARY KEY (user_id)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COMMENT='用户画像';
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=500 \
-Dmapreduce.job.queuename=default \
-Dmapreduce.map.memory.mb=12288 \
-Dmapreduce.reduce.memory.mb=12288 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_buyer_portrait_d_new \
--m 1 \
--update-key "user_id" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_buyer_portrait_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_buyer_portrait_d to themis.ads_buyer_portrait_d_pre,themis.ads_buyer_portrait_d_new to themis.ads_buyer_portrait_d;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
