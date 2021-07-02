#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date +%Y-%m-%d)
fi

echo ${pre_date}

file_num=`aws s3 ls s3://vova-mlb/REC/data/match/match_result/graph_embedding/brand_serial/pt=${pre_date}/ | wc -l`
if [ ${file_num} -eq 0 ]; then
  echo "pt=${pre_date} file num = 0"
  exit 1
fi

hive -e "msck repair table mlb.mlb_vova_graph_embedding_no_brand_data;"

if [ $? -ne 0 ];then
  exit 1
fi


mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
drop table if exists themis.ads_rec_m_graph_embedding_nb_d_new;
drop table if exists themis.ads_rec_m_graph_embedding_nb_d_pre;
CREATE TABLE themis.ads_rec_m_graph_embedding_nb_d_new
(
    goods_id      bigint COMMENT 'goods_id',
    rec_goods_id_list      varchar(4096) COMMENT '推荐商品列表',
    score_list    varchar(4096) COMMENT '推荐商品分数列表',
    cur_date TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0)  COMMENT '日期',
    PRIMARY KEY (goods_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='graph_embedding召回结果表';
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_rec_m_graph_embedding_nb_d_new \
--m 1 \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_graph_embedding_no_brand_data \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--columns goods_id,rec_goods_id_list,score_list \
--fields-terminated-by '\t'

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_rec_m_graph_embedding_nb_d to themis.ads_rec_m_graph_embedding_nb_d_pre;
rename table themis.ads_rec_m_graph_embedding_nb_d_new to themis.ads_rec_m_graph_embedding_nb_d;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



