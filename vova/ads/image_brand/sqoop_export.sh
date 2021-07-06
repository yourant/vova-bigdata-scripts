#!/bin/bash
#指定日期和引擎
pre_date=$1

#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
echo ${pre_date}

uri="ab81e133a11e611ebbee60ebf226a60d-1866282236.us-east-1.elb.amazonaws.com"
dataRow='{
  "data":{
    "jname":"ads_vova_brand_pcv",
    "from":"cv",
    "to":"data",
    "valid_hour":1
  }
}'
# echo "${dataRow}"
resp=`curl ${uri}/vova/api/jobmss/get -s -H "Content-Type:application/json" -X POST --data-raw "${dataRow}"`
echo ${resp}

freedoms=`echo $resp | jq '.data' | jq '.freedoms' | sed -e 's/^"//' -e 's/"$//' | sed 's.\\\\..g'`
echo ${freedoms}
pt=`echo ${freedoms} | jq '.dt' | sed $'s/\"//g'`
echo "pt: ${pt}"

echo "pre_date: ${pre_date}"
#默认日期为昨天
if [ $pt = "null" ]; then
  echo "pt IS NULL"
else
  pre_date=${pt}
fi

echo "pre_date: ${pre_date}"

file_num=`aws s3 ls s3://vova-computer-vision/product_data/vova_image_brand/dst_data/pt=${pre_date}/ | wc -l`
if [ ${file_num} -eq 0 ]; then
  echo "Error: pt=${pre_date} file num = 0"
  exit 1
fi
echo "pt=${pre_date} file num: ${file_num}"

hive -e "msck repair table ads.ads_vova_image_brand_d;"
if [ $? -ne 0 ];then
  exit 1
fi

cnt=$(spark-sql -e "select count(*) from ads.ads_vova_image_brand_d where pt ='${pre_date}';" |tail -1)
if [ ${cnt} -le 0 ];then
  echo "Error: count(*)=${cnt} -le 0"
  exit 1
fi
echo "pt ='${pre_date}' cnt: ${cnt}"

spark-sql \
--conf "spark.app.name=ads_vova_image_brand_d_export_chenkai" \
-e "
insert overwrite table ads.ads_vova_image_brand_d_export
select /*+ REPARTITION(1) */
distinct
  goods_id ,
  brand_id ,
  0 is_update
from
(
  select
    rgps.goods_id ,
    t1.brand_id ,
    row_number() over(partition by rgps.goods_id order by t1.brand_id desc) row
  from
  (
    select
      distinct rgps.group_id, aibd.brand_id
    from
      ads.ads_vova_image_brand_d aibd
    left join
      ods_vova_vbts.ods_vova_rec_gid_pic_similar rgps
    on aibd.goods_id = rgps.goods_id
    where pt ='${pre_date}' and brand_id != -1
  ) t1
  left join
    ods_vova_vbts.ods_vova_rec_gid_pic_similar rgps
  on t1.group_id = rgps.group_id
)
where row = 1 and goods_id > 0 and brand_id > 0
;
"

if [ $? -ne 0 ];then
  exit 1
fi

# mysql 建表
sql="
create table if not exists als_images.ads_vova_image_brand_d (
  id                    bigint(20)  NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  goods_id              bigint(11)  NOT NULL COMMENT '商品id',
  brand_id              bigint(11)  NOT NULL COMMENT '品牌id',
  is_update             int(1)      DEFAULT 0 COMMENT '是否更新，默认0',

  update_time           datetime    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (id) USING BTREE,
  UNIQUE KEY goods_id (goods_id) USING BTREE,
  KEY update_time (update_time) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='brand图像识别接入后台打标'
;
"
# mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx
mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u dwwriter -pwH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx -e "${sql}"

if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.map.memory.mb=8096 \
-Dsqoop.export.records.per.statement=500 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/als_images \
--username dwwriter --password wH7NTzzgVpn8rMAccv0J4Hq3zWM1tylx \
--m 1 \
--table ads_vova_image_brand_d \
--hcatalog-database ads \
--hcatalog-table ads_vova_image_brand_d_export \
--columns goods_id,brand_id,is_update \
--update-key goods_id \
--update-mode allowinsert \
--fields-terminated-by '\t'

if [ $? -ne 0 ];then
  exit 1
fi
