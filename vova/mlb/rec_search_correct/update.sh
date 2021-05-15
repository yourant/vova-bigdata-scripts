#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo ${pre_date}

search_correct_word_d=`aws s3 ls s3://vova-mlb/REC/data/search/correct/mlb_vova_search_correct_word_d/pt=${pre_date}/ | wc -l`
if [ ${search_correct_word_d} -eq 0 ]; then
  echo "pt=${pre_date} search_correct_word_d num = 0"
  exit 1
fi


hive -e "msck repair table mlb.mlb_vova_search_correct_word_d;"


if [ $? -ne 0 ];then
  exit 1
fi

#search_hard_correct_d=`aws s3 ls s3://vova-mlb/REC/data/search/correct/mlb_vova_search_hard_correct_d/pt=${pre_date}/ | wc -l`
#if [ ${search_hard_correct_d} -eq 0 ]; then
#  echo "pt=${pre_date} search_hard_correct_d num = 0"
#  exit 1
#fi
#
#
#hive -e "msck repair table mlb.mlb_vova_search_hard_correct_d;"
#
#
#if [ $? -ne 0 ];then
#  exit 1
#fi


search_correct_gram_d=`aws s3 ls s3://vova-mlb/REC/data/search/correct/mlb_vova_search_correct_gram_d/pt=${pre_date}/ | wc -l`
if [ ${search_correct_gram_d} -eq 0 ]; then
  echo "pt=${pre_date} search_correct_gram_d num = 0"
  exit 1
fi


hive -e "msck repair table mlb.mlb_vova_search_correct_gram_d;"


if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
drop table if exists themis.mlb_vova_search_correct_word_d_new;
drop table if exists themis.mlb_vova_search_correct_word_d_pre;
CREATE TABLE themis.mlb_vova_search_correct_word_d_new (
  id int(11) NOT NULL AUTO_INCREMENT,
  word varchar(128) NOT NULL COMMENT '正确、不需要纠错的单词',
  PRIMARY KEY (id),
  KEY idx_word (word) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table mlb_vova_search_correct_word_d_new \
--m 1 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_search_correct_word_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns word \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.mlb_vova_search_correct_word_d to themis.mlb_vova_search_correct_word_d_pre;
rename table themis.mlb_vova_search_correct_word_d_new to themis.mlb_vova_search_correct_word_d;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi




mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
drop table if exists themis.mlb_vova_search_hard_correct_d_new;
drop table if exists themis.mlb_vova_search_hard_correct_d_pre;
CREATE TABLE themis.mlb_vova_search_hard_correct_d_new (
  id int(11) NOT NULL AUTO_INCREMENT,
  src_word varchar(128) DEFAULT NULL COMMENT '原词',
  word varchar(255) DEFAULT NULL COMMENT '用于替换原词的单词',
  PRIMARY KEY (id),
  KEY idx_src_word (src_word) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table mlb_vova_search_hard_correct_d_new \
--m 1 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_search_hard_correct_d \
--columns src_word,word \
--fields-terminated-by ','

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.mlb_vova_search_hard_correct_d to themis.mlb_vova_search_hard_correct_d_pre;
rename table themis.mlb_vova_search_hard_correct_d_new to themis.mlb_vova_search_hard_correct_d;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
drop table if exists themis.mlb_vova_search_correct_gram_d_new;
drop table if exists themis.mlb_vova_search_correct_gram_d_pre;
CREATE TABLE themis.mlb_vova_search_correct_gram_d_new (
  id int(11) NOT NULL AUTO_INCREMENT,
  src_word varchar(128) DEFAULT NULL COMMENT '前词',
  dist_word varchar(128) DEFAULT NULL COMMENT '后词',
  prob float DEFAULT NULL COMMENT '前后词搭配的概率',
  PRIMARY KEY (id),
  KEY idx_src_dist_word (src_word,dist_word) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table mlb_vova_search_correct_gram_d_new \
--m 1 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_search_correct_gram_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns src_word,dist_word,prob \
--fields-terminated-by ','

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.mlb_vova_search_correct_gram_d to themis.mlb_vova_search_correct_gram_d_pre;
rename table themis.mlb_vova_search_correct_gram_d_new to themis.mlb_vova_search_correct_gram_d;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi




