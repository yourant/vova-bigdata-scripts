#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}"

file_num=`aws s3 ls s3://vova-mlb/REC/data/match/match_result/lightgcn/i2i/no_brand_serial/pt=${pre_date}/ | wc -l`
if [ ${file_num} -eq 0 ]; then
  echo "pt=${pre_date} file num = 0"
  exit 1
fi

hive -e "msck repair table mlb.mlb_vova_rec_m_lightgcn_i2i_nb_d;"
if [ $? -ne 0 ];then
  exit 1
fi

cnt=$(spark-sql -e "select count(*) from mlb.mlb_vova_rec_m_lightgcn_i2i_nb_d where pt ='${pre_date}';" |tail -1)
if [ ${cnt} -le 0 ];then
  echo "Error: count(*)=${cnt} -le 0"
  exit 1
fi
echo ${cnt}

sql="
drop table if exists rec_recall.mlb_vova_rec_m_lightgcn_i2i_nb_d_pre;
drop table if exists rec_recall.mlb_vova_rec_m_lightgcn_i2i_nb_d_new;
create table rec_recall.mlb_vova_rec_m_lightgcn_i2i_nb_d_new(
  \`id\`                int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  \`goods_id\`          bigint NOT NULL COMMENT '商品id',
  \`rec_goods_id_list\` text   NOT NULL COMMENT 'base64 编码后的goods_id list',
  \`score_list\`        text   NOT NULL COMMENT 'base64 编码后的goods_scores list',
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='lightgcn_i2i 非brand召回结果表';

create table if not exists rec_recall.mlb_vova_rec_m_lightgcn_i2i_nb_d (
  \`id\`                int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  \`goods_id\`          bigint NOT NULL COMMENT '商品id',
  \`rec_goods_id_list\` text   NOT NULL COMMENT 'base64 编码后的goods_id list',
  \`score_list\`        text   NOT NULL COMMENT 'base64 编码后的goods_scores list',
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='lightgcn_i2i 非brand召回结果表';
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pv5NxDS1N007jbIISAvB7yzJg2GSbL9zF -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username bimaster --password v5NxDS1N007jbIISAvB7yzJg2GSbL9zF \
--m 1 \
--table mlb_vova_rec_m_lightgcn_i2i_nb_d_new \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_rec_m_lightgcn_i2i_nb_d \
--columns goods_id,rec_goods_id_list,score_list \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pv5NxDS1N007jbIISAvB7yzJg2GSbL9zF <<EOF
rename table rec_recall.mlb_vova_rec_m_lightgcn_i2i_nb_d to rec_recall.mlb_vova_rec_m_lightgcn_i2i_nb_d_pre,rec_recall.mlb_vova_rec_m_lightgcn_i2i_nb_d_new to rec_recall.mlb_vova_rec_m_lightgcn_i2i_nb_d;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
