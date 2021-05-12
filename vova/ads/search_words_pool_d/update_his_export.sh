#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_week=`date -d "6 day ago ${cur_date}" +%Y-%m-%d`

sql="
drop table if exists rec_themis.search_words_pool_new;
drop table if exists rec_themis.search_words_pool_pre;
"
mysql -h rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwthemiswriter -pZ1OtPRLkrUIusK0EeYO9Xjha7a79oToz -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS rec_themis.search_words_pool_new (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id bigint(20) unsigned NOT NULL COMMENT '商品id',
  goods_sn varchar(60) NOT NULL DEFAULT '',
  query varchar(256) NOT NULL DEFAULT '',
  query_count bigint(20) NOT NULL DEFAULT '0' COMMENT '查询数量',
  goods_count bigint(20) NOT NULL DEFAULT '0' COMMENT '物品数量',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY ux_query (goods_id,goods_sn,query),
  KEY query (query) USING BTREE,
  KEY goods_id (goods_id) USING BTREE,
  KEY goods_sn (goods_sn) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS rec_themis.search_words_pool (
  id int(11) NOT NULL AUTO_INCREMENT,
  goods_id bigint(20) unsigned NOT NULL COMMENT '商品id',
  goods_sn varchar(60) NOT NULL DEFAULT '',
  query varchar(256) NOT NULL DEFAULT '',
  query_count bigint(20) NOT NULL DEFAULT '0' COMMENT '查询数量',
  goods_count bigint(20) NOT NULL DEFAULT '0' COMMENT '物品数量',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY ux_query (goods_id,goods_sn,query),
  KEY query (query) USING BTREE,
  KEY goods_id (goods_id) USING BTREE,
  KEY goods_sn (goods_sn) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
"

mysql -h rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwthemiswriter -pZ1OtPRLkrUIusK0EeYO9Xjha7a79oToz -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=default \
--connect jdbc:mysql://rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_themis \
--username dwthemiswriter --password Z1OtPRLkrUIusK0EeYO9Xjha7a79oToz \
--table search_words_pool_new \
--update-key "goods_id,goods_sn,query" \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_search_words_pool_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--columns "goods_id,goods_sn,query,goods_count,query_count"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "----------开始rename-------"
mysql -h rec-themis.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwthemiswriter -pZ1OtPRLkrUIusK0EeYO9Xjha7a79oToz <<EOF
rename table rec_themis.search_words_pool to rec_themis.search_words_pool_pre,rec_themis.search_words_pool_new to rec_themis.search_words_pool;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi