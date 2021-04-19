#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
insert overwrite table ads.ads_vova_mct_black_list_d PARTITION (pt = '${pre_date}')
select
/*+ REPARTITION(1) */
mct_id
from dim.dim_vova_merchant
where spsor_name ='mogu';
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--conf "spark.dynamicAllocation.maxExecutors=5" \
--conf "spark.app.name=ads_mct_black_list_d_zhangyin" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
drop table if exists themis.ads_vova_mct_black_list_new;
drop table if exists themis.ads_vova_mct_black_list_pre;
"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sql="
CREATE TABLE IF NOT EXISTS themis.ads_vova_mct_black_list_new (
  mct_id int(11)  NOT NULL DEFAULT '0' COMMENT '商家id',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (mct_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商家黑名单';

CREATE TABLE IF NOT EXISTS themis.ads_vova_mct_black_list (
  mct_id int(11)  NOT NULL DEFAULT '0' COMMENT '商家id',
  last_update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
  PRIMARY KEY (mct_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商家黑名单';
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--update-mode allowinsert \
--m 1 \
--table ads_vova_mct_black_list_new \
--hcatalog-database ads \
--hcatalog-table ads_vova_mct_black_list_d \
--update-key mct_id \
--columns mct_id \
--hcatalog-partition-keys pt  \
--hcatalog-partition-values  ${pre_date} \
--fields-terminated-by '\001' \

if [ $? -ne 0 ];then
  exit 1
fi
echo "----------开始rename-------"
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table themis.ads_vova_mct_black_list to themis.ads_vova_mct_black_list_pre,themis.ads_vova_mct_black_list_new to themis.ads_vova_mct_black_list;
EOF
echo "-------rename结束--------"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
