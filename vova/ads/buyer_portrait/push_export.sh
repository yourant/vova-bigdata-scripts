#!/bin/bash
#指定日期和引擎
pt=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pt=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
drop table if exists themis_logistics_report.user_push_portrait_new;
drop table if exists themis_logistics_report.user_push_portrait_pre;
"
mysql -h db-logistics-w.gitvv.com -u vvreport4vv -pnTTPdJhVp\!DGv5VX4z33Fw@tHLmIG8oS -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS  themis_logistics_report.user_push_portrait_new (
  user_id int(11)  NOT NULL COMMENT '用户id',
  email varchar(60)  COMMENT '邮箱',
  gender varchar(16)  COMMENT '性别',
  region_code varchar(16)  COMMENT '国家',
  language varchar(16)  COMMENT '语言',
  user_age_group varchar(16)  COMMENT '年龄段',
  utc int(4) NOT NULL COMMENT '时区',
  reg_tag varchar(16)  COMMENT '注册时长',
  buyer_act varchar(16)  COMMENT '用户活跃度',
  trade_act varchar(16)  COMMENT '交易阶段',
  price_prefer varchar(16)  COMMENT '价格偏好',
  goods_id_a int(11)  COMMENT '用户偏好商品A',
  goods_name_a varchar(1024) COMMENT '用户偏好商品A对应语言的标题',
  goods_thumb_a varchar(255)  COMMENT '用户偏好商品A对应语言的主图url',
  goods_keywords_a varchar(255)  COMMENT '用户偏好商品A对应语言的关键词',
  goods_id_b int(11)  COMMENT '用户偏好商品B',
  goods_name_b varchar(1024)  COMMENT '用户偏好商品B对应语言的标题',
  goods_thumb_b varchar(255) COMMENT '用户偏好商品B对应语言的主图url',
  goods_keywords_b varchar(255)  COMMENT '用户偏好商品B对应语言的关键词',
  last_logint_type tinyint COMMENT '上次登入间隔类型',
  last_buyer_type tinyint COMMENT '上次购买间隔类型',
  buy_times_type tinyint COMMENT '近90天购买频率',
  email_act  tinyint  COMMENT '邮箱活跃度',
  PRIMARY KEY (user_id),
  KEY region_code (region_code) USING BTREE,
  KEY language (language) USING BTREE,
  KEY user_age_group (user_age_group) USING BTREE,
  KEY idx_utc_user (utc,user_id) USING BTREE,
  KEY reg_tag (reg_tag) USING BTREE,
  KEY buyer_act (buyer_act) USING BTREE,
  KEY trade_act (trade_act) USING BTREE,
  KEY price_prefer (price_prefer) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='推送用户画像';

CREATE TABLE IF NOT EXISTS themis_logistics_report.user_push_portrait (
  user_id int(11)  NOT NULL COMMENT '用户id',
  email varchar(60)  COMMENT '邮箱',
  gender varchar(16)  COMMENT '性别',
  region_code varchar(16)  COMMENT '国家',
  language varchar(16)  COMMENT '语言',
  user_age_group varchar(16)  COMMENT '年龄段',
  utc int(4) NOT NULL COMMENT '时区',
  reg_tag varchar(16)  COMMENT '注册时长',
  buyer_act varchar(16)  COMMENT '用户活跃度',
  trade_act varchar(16)  COMMENT '交易阶段',
  price_prefer varchar(16)  COMMENT '价格偏好',
  goods_id_a int(11)  COMMENT '用户偏好商品A',
  goods_name_a varchar(1024) COMMENT '用户偏好商品A对应语言的标题',
  goods_thumb_a varchar(255)  COMMENT '用户偏好商品A对应语言的主图url',
  goods_keywords_a varchar(255)  COMMENT '用户偏好商品A对应语言的关键词',
  goods_id_b int(11)  COMMENT '用户偏好商品B',
  goods_name_b varchar(1024)  COMMENT '用户偏好商品B对应语言的标题',
  goods_thumb_b varchar(255) COMMENT '用户偏好商品B对应语言的主图url',
  goods_keywords_b varchar(255)  COMMENT '用户偏好商品B对应语言的关键词',
  last_logint_type tinyint COMMENT '上次登入间隔类型',
  last_buyer_type tinyint COMMENT '上次购买间隔类型',
  buy_times_type tinyint COMMENT '近90天购买频率',
  email_act  tinyint  COMMENT '邮箱活跃度',
  PRIMARY KEY (user_id),
  KEY region_code (region_code) USING BTREE,
  KEY language (language) USING BTREE,
  KEY user_age_group (user_age_group) USING BTREE,
  KEY idx_utc_user (utc,user_id) USING BTREE,
  KEY reg_tag (reg_tag) USING BTREE,
  KEY buyer_act (buyer_act) USING BTREE,
  KEY trade_act (trade_act) USING BTREE,
  KEY price_prefer (price_prefer) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='推送用户画像';
"

mysql -h db-logistics-w.gitvv.com -u vvreport4vv -pnTTPdJhVp\!DGv5VX4z33Fw@tHLmIG8oS -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
-Dmapreduce.map.memory.mb=8192 \
-Dmapreduce.reduce.memory.mb=8192 \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mariadb:aurora://db-logistics-w.gitvv.com:3306/themis_logistics_report \
--username vvreport4vv --password 'nTTPdJhVp!DGv5VX4z33Fw@tHLmIG8oS' \
--connection-manager org.apache.sqoop.manager.MySQLManager \
--table user_push_portrait_new \
--m 20 \
--hcatalog-database ads \
--hcatalog-table ads_vova_buyer_push_portrait \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h db-logistics-w.gitvv.com -u vvreport4vv -pnTTPdJhVp\!DGv5VX4z33Fw@tHLmIG8oS <<EOF
rename table themis_logistics_report.user_push_portrait to themis_logistics_report.user_push_portrait_pre,themis_logistics_report.user_push_portrait_new to themis_logistics_report.user_push_portrait;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
