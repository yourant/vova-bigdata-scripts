#!/bin/bash
# 由 job messager 启动的任务, 会有 freedoms
freedoms=$1
echo "freedoms: ${freedoms}"

if [ ! -n "$1" ]; then
  echo "Error: freedoms 为必传参数！！！"
  exit 1
fi

# 从 freedoms 拿到 table_name 和 dt
table_name=`echo $freedoms | jq '.table_name' | sed $'s/\"//g'`
pre_date=`echo $freedoms | jq '.dt' | sed $'s/\"//g'`

if [ ! -n "${table_name}" ]; then
  echo "Error: 任务: mlb_sqoop_u2i, freedoms 中没有配置 table_name！！！"
  exit 1
fi

if [ ! -n "${pre_date}" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "final pre_date： ${pre_date}"
echo "final table_name： ${table_name}"

# 判断对应表、对应分区 是否有数据
hive -e "msck repair table mlb.${table_name};"
if [ $? -ne 0 ];then
  exit 1
fi

cnt=$(spark-sql -e "select count(*) from mlb.${table_name} where pt ='${pre_date}';" |tail -1)
if [ ${cnt} -le 0 ];then
  echo "Error: ${table_name}, pt=${pre_date}, 数据条数异常 count(*)=${cnt} -le 0"
  exit 1
fi
echo ${cnt}

# mysql 建表
sql="
drop table if exists rec_recall.${table_name}_pre;
drop table if exists rec_recall.${table_name}_new;
create table rec_recall.${table_name}_new(
  \`id\`                int(11)        NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  \`buyer_id\`          bigint         NOT NULL COMMENT '用户ID',
  \`rec_goods_list\`    text           NOT NULL COMMENT 'base64 编码后的goods_id list',
  \`score_list\`        text           NOT NULL COMMENT 'base64 编码后的goods_scores list',
  \`model_name\`        varchar(100)   NOT NULL COMMENT '模型名',
  \`update_time\`       datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY buyer_id (buyer_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='u2i 召回结果表';

create table if not exists rec_recall.${table_name} (
  \`id\`                int(11)        NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  \`buyer_id\`          bigint         NOT NULL COMMENT '用户ID',
  \`rec_goods_list\`    text           NOT NULL COMMENT 'base64 编码后的goods_id list',
  \`score_list\`        text           NOT NULL COMMENT 'base64 编码后的goods_scores list',
  \`model_name\`        varchar(100)   NOT NULL COMMENT '模型名',
  \`update_time\`       datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (\`id\`) USING BTREE,
  UNIQUE KEY buyer_id (buyer_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='u2i 召回结果表';
"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

# 同步到 mysql
sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--m 1 \
--table ${table_name}_new \
--hcatalog-database mlb \
--hcatalog-table ${table_name} \
--columns buyer_id,rec_goods_list,score_list,model_name \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pre_date} \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  echo "sqoop export Error: ${table_name}, pt=${pre_date}"
  exit 1
fi

# rename mysql table

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwrecallwriter -pTsLdpZumzovrAvttIqnePCJhIVxZZ7bd <<EOF
rename table rec_recall.${table_name} to rec_recall.${table_name}_pre,rec_recall.${table_name}_new to rec_recall.${table_name};
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
