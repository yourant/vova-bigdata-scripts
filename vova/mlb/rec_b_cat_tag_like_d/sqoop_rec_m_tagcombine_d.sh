#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}"

file_num=`aws s3 ls s3://vova-mlb/REC/data/match/new_user/recall_result_data/mlb_rec_m_tagcombine_d/pt=${pre_date}/ | wc -l`
if [ ${file_num} -eq 0 ]; then
  echo "还没有数据,不执行导数任务, pt=${pre_date} number of files is 0"
  exit 1
fi

hive -e "msck repair table mlb.mlb_rec_m_tagcombine_d;"
if [ $? -ne 0 ];then
  exit 1
fi

cnt=$(spark-sql -e "select count(*) from mlb.mlb_rec_m_tagcombine_d where pt ='${pre_date}';" |tail -1)
if [ ${cnt} -le 0 ];then
  echo "Error: 当天分区 count(*)=${cnt} -le 0, 不能导数"
  exit 1
fi
echo ${cnt}

sql="
drop table if exists rec_recall.mlb_rec_m_tagcombine_d_pre;
drop table if exists rec_recall.mlb_rec_m_tagcombine_d_new;
create table if not exists rec_recall.mlb_rec_m_tagcombine_d_new (
id              int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
region_id       bigint        NOT NULL COMMENT '区域',
gender          varchar(100)  NOT NULL COMMENT '性别',
user_age_group  varchar(100)  NOT NULL COMMENT '用户年龄组',
kg_tag_combine  varchar(200)  NOT NULL COMMENT '标签组合',
tag_score       double        NOT NULL COMMENT '标签打分',
PRIMARY KEY (id) USING BTREE,
KEY region_id (region_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='知识图谱偏好召回';

create table if not exists rec_recall.mlb_rec_m_tagcombine_d (
id              int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
region_id       bigint        NOT NULL COMMENT '区域',
gender          varchar(100)  NOT NULL COMMENT '性别',
user_age_group  varchar(100)  NOT NULL COMMENT '用户年龄组',
kg_tag_combine  varchar(200)  NOT NULL COMMENT '标签组合',
tag_score       double        NOT NULL COMMENT '标签打分',
PRIMARY KEY (id) USING BTREE,
KEY region_id (region_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='知识图谱偏好召回';
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
--table mlb_rec_m_tagcombine_d_new \
--hcatalog-database mlb \
--hcatalog-table mlb_rec_m_tagcombine_d \
--columns region_id,gender,user_age_group,kg_tag_combine,tag_score \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pv5NxDS1N007jbIISAvB7yzJg2GSbL9zF <<EOF
rename table rec_recall.mlb_rec_m_tagcombine_d to rec_recall.mlb_rec_m_tagcombine_d_pre,rec_recall.mlb_rec_m_tagcombine_d_new to rec_recall.mlb_rec_m_tagcombine_d;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
