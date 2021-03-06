#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 hour" +%Y/%m/%d/%H`
echo "pt=${cur_date}"
fi
max_pt=$(hive -e "show partitions ads.ads_vova_goods_id_behave_m" | tail -1)
if [ $? -ne 0 ];then
  exit 1
fi
last_pt=${max_pt:3}
echo "last_pt=$last_pt"
pt=`date -d "-1 hour" +%Y-%m-%d`
echo "$pt"

spark-submit \
--master yarn \
--executor-memory 6G \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf spark.app.name=ads_vova_min_price_goods_h_zhangyin \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.MinPrice s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--env product --pt $cur_date --last_pt $last_pt

if [ $? -ne 0 ];then
  echo "goods_pic_similar job error"
  exit 1
fi

sql="
drop table if exists themis.ads_min_price_goods_h_new;
drop table if exists themis.ads_min_price_goods_h_pre;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis.ads_min_price_goods_h_new (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  goods_id int(11) NOT NULL,
  min_price_goods_id int(11) NOT NULL,
  strategy varchar(16) NOT NULL,
  group_number varchar(32) NOT NULL,
  update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  min_show_price decimal(14,4) DEFAULT NULL COMMENT '最低价',
  avg_sku_price decimal(14,4) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY ux_goods_id (goods_id,strategy),
  KEY goods_id (goods_id),
  KEY group_number (group_number)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS themis.ads_min_price_goods_h (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  goods_id int(11) NOT NULL,
  min_price_goods_id int(11) NOT NULL,
  strategy varchar(16) NOT NULL,
  group_number varchar(32) NOT NULL,
  update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  min_show_price decimal(14,4) DEFAULT NULL COMMENT '最低价',
  avg_sku_price decimal(14,4) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY ux_goods_id (goods_id,strategy),
  KEY goods_id (goods_id),
  KEY group_number (group_number)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8192 \
-Dmapreduce.reduce.memory.mb=8192 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--table ads_min_price_goods_h_new \
--update-key "goods_id,strategy" \
--m 1 \
--update-mode allowinsert \
--hcatalog-database ads \
--hcatalog-table ads_vova_min_price_goods_h \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${pt} \
--fields-terminated-by '\001' \
--columns "goods_id,min_price_goods_id,strategy,group_number,min_show_price,avg_sku_price" \
--verbose

if [ $? -ne 0 ];then
  echo "ads_min_price_goods_h sqoop error"
  exit 1
fi

cnt=$(mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "select count(id) from themis.ads_min_price_goods_h_new;" |tail -1)
echo ${cnt}
if [ ${cnt} -le 0 ];then
  echo "Error: count(*)=${cnt} -le 0"
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx <<EOF
rename table themis.ads_min_price_goods_h to themis.ads_min_price_goods_h_pre,themis.ads_min_price_goods_h_new to themis.ads_min_price_goods_h;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

