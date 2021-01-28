#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "${pre_date}"

file_num=`aws s3 ls s3://vova-computer-vision/product_data/vova_best_sale_banner/goods_banner/tab/pt=${pre_date}/ | wc -l`
if [ ${file_num} -eq 0 ]; then
  echo "pt=${pre_date} file num = 0"
  exit 1
fi

spark-sql --conf "spark.app.name=ads_best_sale_goods_banner" -e "
msck repair table ads.ads_best_sale_goods_banner;
drop table if exists tmp.ads_best_sale_goods_banner;
create table tmp.ads_best_sale_goods_banner as
select
/*+ REPARTITION(1) */
t.goods_id,
t.img_id,
g.languages_id,
t.banner_url
from ads.ads_best_sale_goods_banner t
left join dwd.dim_languages g on t.language = g.languages_code
where t.pt='$pre_date';
"
if [ $? -ne 0 ];then
  exit 1
fi

sql="
drop table if exists rec_recall.ads_best_sale_goods_banner_pre;
drop table if exists rec_recall.ads_best_sale_goods_banner_new;

create table rec_recall.ads_best_sale_goods_banner_new (
    id             int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    goods_id       int(11)       NOT NULL COMMENT '商品id',
    img_id         int(11)       NOT NULL COMMENT '图片id',
    languages_id   int(4)        NOT NULL COMMENT '语言ID',
    banner_url     varchar(255)  NOT NULL COMMENT 'banner_url',
    PRIMARY KEY (id) USING BTREE,
    KEY goods_id (goods_id) USING BTREE,
    UNIQUE KEY ux_goods_id (goods_id, languages_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='女装类目图谱热销商品banner';

create table if not exists rec_recall.ads_best_sale_goods_banner (
    id             int(11)       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    goods_id       int(11)       NOT NULL COMMENT '商品id',
    img_id         int(11)       NOT NULL COMMENT '图片id',
    languages_id   int(4)        NOT NULL COMMENT '语言ID',
    banner_url     varchar(255)  NOT NULL COMMENT 'banner_url',
    PRIMARY KEY (id) USING BTREE,
    KEY goods_id (goods_id) USING BTREE,
    UNIQUE KEY ux_goods_id (goods_id, languages_id) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='女装类目图谱热销商品banner';
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
--table ads_best_sale_goods_banner_new \
--hcatalog-database tmp \
--hcatalog-table ads_best_sale_goods_banner \
--columns goods_id,img_id,languages_id,banner_url \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

echo "----------开始rename-------"
mysql -h rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pv5NxDS1N007jbIISAvB7yzJg2GSbL9zF <<EOF
rename table rec_recall.ads_best_sale_goods_banner to rec_recall.ads_best_sale_goods_banner_pre,rec_recall.ads_best_sale_goods_banner_new to rec_recall.ads_best_sale_goods_banner;
EOF
echo "-------rename结束--------"

if [ $? -ne 0 ];then
  exit 1
fi
