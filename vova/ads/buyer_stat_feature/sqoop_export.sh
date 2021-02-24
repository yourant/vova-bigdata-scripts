#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
drop table if exists themis.ads_buyer_stat_feature_pre;
drop table if exists themis.ads_buyer_stat_feature_new;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_buyer_stat_feature_new\` (
  \`id\`                        int(11)              NOT NULL AUTO_INCREMENT,
  \`buyer_id\`                  int(11)              NOT NULL          COMMENT 'i_用户id',
  \`reg_gender\`                VARCHAR(10)          COMMENT 'd_注册性别',
  \`reg_age_group\`             VARCHAR(10)          COMMENT 'd_注册年龄',
  \`reg_ctry\`                  VARCHAR(10)          COMMENT 'd_注册国家',
  \`reg_time\`                  TIMESTAMP            COMMENT 'd_注册时间',
  \`reg_channel\`               VARCHAR(50)          COMMENT 'd_注册渠道',
  \`os_type\`                   VARCHAR(10)          COMMENT 'd_系统类型',
  \`first_cat_likes\`           VARCHAR(50)          COMMENT 'd_一级品类偏好Top3',
  \`second_cat_likes\`          VARCHAR(50)          COMMENT 'd_二级品类偏好Top3',
  \`first_order_time\`          TIMESTAMP            COMMENT 'd_首单时间',
  \`last_order_time\`           TIMESTAMP            COMMENT 'd_最近下单时间',
  \`order_cnt\`                 int(11)              COMMENT 'd_购买订单数',
  \`avg_price\`                 decimal(13,2)        COMMENT 'd_笔单价',
  \`price_range\`               int(1)               COMMENT 'd_价格偏好层级',
  \`buyer_act\`                 VARCHAR(20)          COMMENT 'd_活跃度',
  \`trade_act\`                 VARCHAR(20)          COMMENT 'd_交易阶段',
  \`last_logint_type\`          int(1)               COMMENT 'd_上次登入时间',
  \`last_buyer_type\`           int(1)               COMMENT 'd_上次购买时间',
  \`buy_times_type\`            int(1)               COMMENT 'd_消费频率',
  \`email_act\`                 int(1)               COMMENT 'd_EDM-邮件分组',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`buyer_id_key\` (\`buyer_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
CREATE TABLE IF NOT EXISTS \`themis\`.\`ads_buyer_stat_feature\` (
  \`id\`                        int(11)              NOT NULL AUTO_INCREMENT,
  \`buyer_id\`                  int(11)              NOT NULL          COMMENT 'i_用户id',
  \`reg_gender\`                VARCHAR(10)          COMMENT 'd_注册性别',
  \`reg_age_group\`             VARCHAR(10)          COMMENT 'd_注册年龄',
  \`reg_ctry\`                  VARCHAR(10)          COMMENT 'd_注册国家',
  \`reg_time\`                  TIMESTAMP            COMMENT 'd_注册时间',
  \`reg_channel\`               VARCHAR(50)          COMMENT 'd_注册渠道',
  \`os_type\`                   VARCHAR(10)          COMMENT 'd_系统类型',
  \`first_cat_likes\`           VARCHAR(50)          COMMENT 'd_一级品类偏好Top3',
  \`second_cat_likes\`          VARCHAR(50)          COMMENT 'd_二级品类偏好Top3',
  \`first_order_time\`          TIMESTAMP            COMMENT 'd_首单时间',
  \`last_order_time\`           TIMESTAMP            COMMENT 'd_最近下单时间',
  \`order_cnt\`                 int(11)              COMMENT 'd_购买订单数',
  \`avg_price\`                 decimal(13,2)        COMMENT 'd_笔单价',
  \`price_range\`               int(1)               COMMENT 'd_价格偏好层级',
  \`buyer_act\`                 VARCHAR(20)          COMMENT 'd_活跃度',
  \`trade_act\`                 VARCHAR(20)          COMMENT 'd_交易阶段',
  \`last_logint_type\`          int(1)               COMMENT 'd_上次登入时间',
  \`last_buyer_type\`           int(1)               COMMENT 'd_上次购买时间',
  \`buy_times_type\`            int(1)               COMMENT 'd_消费频率',
  \`email_act\`                 int(1)               COMMENT 'd_EDM-邮件分组',
  \`update_time\` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  KEY \`buyer_id_key\` (\`buyer_id\`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPACT;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bimaster --password sYG2Ri3yIDu2NPki \
--m 1 \
--table ads_buyer_stat_feature_new \
--hcatalog-database ads \
--hcatalog-table ads_buyer_stat_feature \
--columns buyer_id,reg_gender,reg_age_group,reg_ctry,reg_time,reg_channel,os_type,first_cat_likes,second_cat_likes,first_order_time,last_order_time,order_cnt,avg_price,price_range,buyer_act,trade_act,last_logint_type,last_buyer_type,buy_times_type,email_act \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki <<EOF
rename table themis.ads_buyer_stat_feature to themis.ads_buyer_stat_feature_pre,themis.ads_buyer_stat_feature_new to themis.ads_buyer_stat_feature;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
