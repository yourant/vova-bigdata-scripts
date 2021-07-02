#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_week=`date -d "6 day ago ${cur_date}" +%Y-%m-%d`

sql="
drop table if exists themis.ads_search_words_pool_new;
drop table if exists themis.ads_search_words_pool_pre;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis.ads_search_words_pool_new (
  goods_id bigint(20) NOT NULL COMMENT '商品id',
  query varchar(128) NOT NULL DEFAULT '',
  language varchar(20) NOT NULL DEFAULT '',
  rank bigint(10) NOT NULL DEFAULT '0' COMMENT '商家等级',
  goods_count bigint(20) NOT NULL DEFAULT '0' COMMENT '物品数量',
  query_count bigint(20) NOT NULL DEFAULT '0' COMMENT '查询数量',
  goods_sn varchar(60) NOT NULL,
  PRIMARY KEY (goods_id,query,language),
  KEY query (query) USING BTREE,
  KEY goods_id (goods_id) USING BTREE,
  KEY goods_count (goods_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS themis.ads_search_words_pool (
  goods_id bigint(20) NOT NULL COMMENT '商品id',
  query varchar(128) NOT NULL DEFAULT '',
  language varchar(20) NOT NULL DEFAULT '',
  rank bigint(10) NOT NULL DEFAULT '0' COMMENT '商家等级',
  goods_count bigint(20) NOT NULL DEFAULT '0' COMMENT '物品数量',
  query_count bigint(20) NOT NULL DEFAULT '0' COMMENT '查询数量',
  goods_sn varchar(60) NOT NULL,
  PRIMARY KEY (goods_id,query,language),
  KEY query (query) USING BTREE,
  KEY goods_id (goods_id) USING BTREE,
  KEY goods_count (goods_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_search_words_pool_new \
--update-key "query,goods_id,language" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_search_words_pool \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001'

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_search_words_pool to themis.ads_search_words_pool_pre,themis.ads_search_words_pool_new to themis.ads_search_words_pool;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi