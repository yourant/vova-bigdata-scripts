#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo ${pre_date}

search_expansion_a_d=`aws s3 ls s3://vova-mlb/REC/data/search/expansion/mlb_vova_search_expansion_a_d/pt=${pre_date}/ | wc -l`
if [ ${search_expansion_a_d} -eq 0 ]; then
  echo "pt=${pre_date} search_expansion_a_d num = 0"
  exit 1
fi


hive -e "msck repair table mlb.mlb_vova_search_expansion_a_d;"


if [ $? -ne 0 ];then
  exit 1
fi


mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
drop table if exists themis.mlb_vova_search_expansion_a_d_new;
drop table if exists themis.mlb_vova_search_expansion_a_d_pre;
CREATE TABLE themis.mlb_vova_search_expansion_a_d_new
(
  id int(11) NOT NULL AUTO_INCREMENT,
  query varchar(128) NOT NULL COMMENT '原始搜索query',
  expansion varchar(128) DEFAULT NULL COMMENT '拓展query',
  score double(11,4) DEFAULT NULL COMMENT '得分',
  last_update_time datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '日期',
  PRIMARY KEY (id),
  KEY ix_query (query) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='搜索词拓展表';
EOF


sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8192 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table mlb_vova_search_expansion_a_d_new \
--m 4 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_search_expansion_a_d \
--columns query,expansion,score \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by ','




if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.mlb_vova_search_expansion_a_d to themis.mlb_vova_search_expansion_a_d_pre;
rename table themis.mlb_vova_search_expansion_a_d_new to themis.mlb_vova_search_expansion_a_d;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

