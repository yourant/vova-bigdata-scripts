#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
pre_week=`date -d "6 day ago ${pre_date}" +%Y-%m-%d`
fi

sql="
drop table if exists themis.ads_country_hot_search_words_new;
drop table if exists themis.ads_country_hot_search_words_pre;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis.ads_country_hot_search_words_new (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  region_id int(11) NOT NULL DEFAULT '0' COMMENT '',
  region_code varchar(32) NOT NULL DEFAULT '0' COMMENT '',
  hot_words varchar(128) NOT NULL DEFAULT '' COMMENT '热搜词',
  create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (id),
  KEY region_id (region_id),
  KEY region_code (region_code),
  KEY hot_words (hot_words)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='国家热搜词topN';

CREATE TABLE IF NOT EXISTS themis.ads_country_hot_search_words (
  id int(11) unsigned NOT NULL AUTO_INCREMENT,
  region_id int(11) NOT NULL DEFAULT '0' COMMENT '',
  region_code varchar(32) NOT NULL DEFAULT '0' COMMENT '',
  hot_words varchar(128) NOT NULL DEFAULT '' COMMENT '热搜词',
  create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (id),
  KEY region_id (region_id),
  KEY region_code (region_code),
  KEY hot_words (hot_words)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin COMMENT='国家热搜词topN';
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bimaster --password sYG2Ri3yIDu2NPki \
--table ads_country_hot_search_words_new \
--m 1 \
--update-key "region_id,region_code,hot_words" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_country_hot_search_words \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001' \
--columns "region_id,region_code,hot_words"


#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -psYG2Ri3yIDu2NPki <<EOF
rename table themis.ads_country_hot_search_words to themis.ads_country_hot_search_words_pre, themis.ads_country_hot_search_words_new to themis.ads_country_hot_search_words;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

