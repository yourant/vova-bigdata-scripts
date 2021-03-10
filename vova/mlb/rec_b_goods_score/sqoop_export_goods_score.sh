#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

# 检查文件是否生成
echo "${pre_date}"
file_num=`aws s3 ls s3://vova-mlb/REC/data/goods_score_data/goods_score/pt=${pre_date}/ | wc -l`
if [ ${file_num} -eq 0 ]; then
  echo "pt=${pre_date} file num = 0"
  exit 1
fi

# 刷新表元数据
hive -e "msck repair table mlb.mlb_vova_rec_b_goods_score_d;"
if [ $? -ne 0 ];then
  exit 1
fi

# 检查分区数据量
cnt=$(spark-sql -e "select count(*) from mlb.mlb_vova_rec_b_goods_score_d where pt ='${pre_date}';" |tail -1)
if [ ${cnt} -le 0 ];then
  echo "Error: count(*)=${cnt} -le 0"
  exit 1
fi
echo ${cnt}

sql="
drop table if exists rec_recall.ads_rec_b_goods_score_d_pre;
drop table if exists rec_recall.ads_rec_b_goods_score_d_new;

create table rec_recall.ads_rec_b_goods_score_d_new (
  \`id\`                bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  \`goods_id\`          int(11)     NOT NULL COMMENT '商品id',
  \`base_score\`        DOUBLE      NOT NULL COMMENT '基础评分',
  \`hot_score\`         DOUBLE      NOT NULL COMMENT '热度评分',
  \`conversion_score\`  DOUBLE      NOT NULL COMMENT '转化评分',
  \`honor_score\`       DOUBLE      NOT NULL COMMENT '履约评分',
  \`overall_score\`     DOUBLE      NOT NULL COMMENT '综合评分',
  \`update_time\`       datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品综合评分表(搜索及mostpopular)'
;

create table if not exists rec_recall.ads_rec_b_goods_score_d (
  \`id\`                bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  \`goods_id\`          int(11)     NOT NULL COMMENT '商品id',
  \`base_score\`        DOUBLE      NOT NULL COMMENT '基础评分',
  \`hot_score\`         DOUBLE      NOT NULL COMMENT '热度评分',
  \`conversion_score\`  DOUBLE      NOT NULL COMMENT '转化评分',
  \`honor_score\`       DOUBLE      NOT NULL COMMENT '履约评分',
  \`overall_score\`     DOUBLE      NOT NULL COMMENT '综合评分',
  \`update_time\`       datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='商品综合评分表(搜索及mostpopular)'
;
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pv5NxDS1N007jbIISAvB7yzJg2GSbL9zF -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username bimaster --password v5NxDS1N007jbIISAvB7yzJg2GSbL9zF \
--m 1 \
--table ads_rec_b_goods_score_d_new \
--hcatalog-database mlb \
--hcatalog-table mlb_vova_rec_b_goods_score_d \
--columns goods_id,base_score,hot_score,conversion_score,honor_score,overall_score \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pv5NxDS1N007jbIISAvB7yzJg2GSbL9zF <<EOF
rename table rec_recall.ads_rec_b_goods_score_d to rec_recall.ads_rec_b_goods_score_d_pre,rec_recall.ads_rec_b_goods_score_d_new to rec_recall.ads_rec_b_goods_score_d;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
