#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
msck repair table ads.fn_ads_img_vector_arc;
select max(pt) from ads.fn_ads_img_vector_arc;
"
res=`spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=check_fn_ads_img_vector_arc" -e "$sql"`
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "$res == $cur_date"
if [ "$res" != "$cur_date" ]; then
  echo "not update"
  exit 1
fi

sql="
INSERT OVERWRITE TABLE ads.fn_ads_img_vector PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(5) */
    event_date,
    goods_id,
    img_id,
    img_original,
    img_vector
FROM (
         SELECT event_date,
                goods_id,
                img_id,
                img_original,
                img_vector,
                row_number() OVER (PARTITION BY t1.goods_id ORDER BY t1.event_date DESC ) rank
         FROM (
                  SELECT pt AS event_date,
                         goods_id,
                         img_id,
                         img_original,
                         img_vector
                  FROM ads.fn_ads_img_vector_arc arc
                  WHERE arc.pt = '${cur_date}'
                  UNION ALL
                  SELECT event_date,
                         goods_id,
                         img_id,
                         img_original,
                         img_vector
                  FROM ads.fn_ads_img_vector all
                  WHERE all.pt = date_sub('${cur_date}', 1)
              ) t1
     ) fin
WHERE fin.rank = 1
;
"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=20" --conf "spark.app.name=fn_ads_img_vector" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists als_images.fn_images_vector_new;
drop table if exists als_images.fn_images_vector_pre;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"

sql="
CREATE TABLE IF NOT EXISTS als_images.fn_images_vector_new
(
    id               int(11)        NOT NULL AUTO_INCREMENT,
    goods_id         bigint(20)     NOT NULL COMMENT 'fn商品id',
    img_vector       text           NOT NULL COMMENT '图片向量',
    last_update_time datetime       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY goods_id (goods_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;
"

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e -e "${sql}"

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dsqoop.export.records.per.statement=1000 \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/als_images \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--table fn_images_vector_new \
--m 1 \
--columns goods_id,img_vector \
--hcatalog-database ads \
--hcatalog-table fn_ads_img_vector \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--fields-terminated-by '\001' \
--batch

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bdwriter -pDd7LvXRPDP4iIJ7FfT8e <<EOF
rename table als_images.fn_images_vector to als_images.fn_images_vector_pre,
             als_images.fn_images_vector_new to als_images.fn_images_vector;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=fn_images_vector_20201113 --from=data --to=java --jtype=1D --retry=0

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


