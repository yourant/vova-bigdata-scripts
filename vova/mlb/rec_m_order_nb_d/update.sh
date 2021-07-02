#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date +%Y-%m-%d)
fi

echo ${pre_date}

order_u2i_num=`aws s3 ls s3://vova-mlb/REC/data/match/match_result/order_u2i/no_brand_serial/pt=${pre_date}/ | wc -l`
if [ ${order_u2i_num} -eq 0 ]; then
  echo "pt=${pre_date} order_u2i num = 0"
  exit 1
fi


hive -e "msck repair table mlb.mlb_vova_rec_m_order_u2i_nb_d;"


if [ $? -ne 0 ];then
  exit 1
fi


mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
drop table if exists themis.rec_m_order_u2i_nb_d_new;
drop table if exists themis.rec_m_order_u2i_nb_d_pre;
CREATE TABLE themis.rec_m_order_u2i_nb_d_new
(
    buyer_id      bigint COMMENT 'buyer_id',
    rec_goods_id_list      varchar(3072) COMMENT 'rec_goods_id_list',
    score_list    varchar(3072) COMMENT 'score_list',
    cur_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)  COMMENT '日期',
    PRIMARY KEY (buyer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='rec_m_order_u2i_nb_d';
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table rec_m_order_u2i_nb_d_new \
--m 1 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_rec_m_order_u2i_nb_d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns buyer_id,rec_goods_id_list,score_list \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.rec_m_order_u2i_nb_d to themis.rec_m_order_u2i_nb_d_pre;
rename table themis.rec_m_order_u2i_nb_d_new to themis.rec_m_order_u2i_nb_d;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi




