#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_month_date=`date -d "29 days ago ${cur_date}" +%Y-%m-%d`
echo "pre_month_date=${pre_month_date}"

###逻辑sql
sql="
INSERT OVERWRITE TABLE ads.ads_vova_buyer_portrait_brand_likes_30d PARTITION (pt = '${cur_date}')
select a.buyer_id,
       a.brand_id,
       c.brand_name,
       a.likes_weight_synth
from ads.ads_vova_buyer_portrait_brand_likes_exp a
         join (select buyer_id from dws.dws_vova_buyer_goods_behave where pt >= '${pre_month_date}' group by buyer_id) b
              on a.buyer_id = b.buyer_id
         left join ods_vova_vts.ods_vova_brand c on a.brand_id = c.brand_id
where a.pt = '${cur_date}' and a.likes_weight_synth > 0.5
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.maxExecutors=120" --conf "spark.app.name=ads_vova_buyer_portrait_brand_likes_30d" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h als-robot-db.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -ujuntaohanwrite -p'h5TaV049mbQxu8dVXBi9hrcpbMvLei3K' <<EOF
drop table if exists als_robot.ads_user_brand_prefences_new;
drop table if exists als_robot.ads_user_brand_prefences_pre;

CREATE TABLE als_robot.ads_user_brand_prefences_new
(
    user_id       bigint(20) NOT NULL COMMENT '买家id',
    brand_id             bigint(20) COMMENT '品牌id',
    brand_name           varchar(60) COMMENT '品牌name',
    score        decimal(10,6) COMMENT '品牌偏好度',
    PRIMARY KEY (user_id,brand_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='近30天商家类目等级数据';
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://als-robot-db.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/als_robot \
--username juntaohanwrite --password h5TaV049mbQxu8dVXBi9hrcpbMvLei3K \
--table ads_user_brand_prefences_new \
--m 1 \
--hcatalog-database ads \
--hcatalog-table ads_vova_buyer_portrait_brand_likes_30d \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--columns user_id,brand_id,brand_name,score \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h als-robot-db.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -ujuntaohanwrite -p'h5TaV049mbQxu8dVXBi9hrcpbMvLei3K' <<EOF
rename table als_robot.ads_user_brand_prefences to als_robot.ads_user_brand_prefences_pre;
rename table als_robot.ads_user_brand_prefences_new to als_robot.ads_user_brand_prefences;
EOF

if [ $? -ne 0 ];then
  exit 1
fi