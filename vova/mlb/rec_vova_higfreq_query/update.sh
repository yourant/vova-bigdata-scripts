#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo ${pre_date}

highfreq_query_mapping=`aws s3 ls s3://vova-mlb/REC/data/match/match_result/mlb_vova_highfreq_query_mapping_d/pt=${pre_date}/ | wc -l`
if [ ${highfreq_query_mapping} -eq 0 ]; then
  echo "pt=${pre_date} highfreq_query_mapping num = 0"
  exit 1
fi


hive -e "msck repair table mlb.mlb_vova_highfreq_query_mapping_d;"


if [ $? -ne 0 ];then
  exit 1
fi

highfreq_query_match=`aws s3 ls s3://vova-mlb/REC/data/match/match_result/mlb_vova_highfreq_query_match_d/pt=${pre_date}/ | wc -l`
if [ ${highfreq_query_match} -eq 0 ]; then
  echo "pt=${pre_date} highfreq_query_match num = 0"
  exit 1
fi


hive -e "msck repair table mlb.mlb_vova_highfreq_query_match_d;"


if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
drop table if exists themis.rec_highfreq_query_mapping_d_new;
drop table if exists themis.rec_highfreq_query_mapping_d_pre;
CREATE TABLE themis.rec_highfreq_query_mapping_d_new
(
    source_origin      varchar(128) COMMENT '原始query',
    target_query      varchar(128) COMMENT '归并映射mapping之后的query',
    cur_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)  COMMENT '日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='搜索词映射表';
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table rec_highfreq_query_mapping_d_new \
--m 1 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_highfreq_query_mapping_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns source_origin,target_query \
--fields-terminated-by '\t'

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.rec_highfreq_query_mapping_d to themis.rec_highfreq_query_mapping_d_pre;
rename table themis.rec_highfreq_query_mapping_d_new to themis.rec_highfreq_query_mapping_d;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi




mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
drop table if exists themis.rec_highfreq_query_match_d_new;
drop table if exists themis.rec_highfreq_query_match_d_pre;
CREATE TABLE themis.rec_highfreq_query_match_d_new
(
    query_keys      varchar(128) COMMENT '归并映射后的query 与性别的组合，例如：query 为 nike, 性别为male 则query_key 为nike@male',
    goods_list      text COMMENT '序列化的商品列表，做大出500条，不足的从根据翻译后query从语义或者ES进行补充',
    cur_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)  COMMENT '日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='搜索词&性别(第一版先使用性别过滤)召回结果表';
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table rec_highfreq_query_match_d_new \
--m 1 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_highfreq_query_match_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns query_keys,goods_list \
--fields-terminated-by '\t'

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.rec_highfreq_query_match_d to themis.rec_highfreq_query_match_d_pre;
rename table themis.rec_highfreq_query_match_d_new to themis.rec_highfreq_query_match_d;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi




