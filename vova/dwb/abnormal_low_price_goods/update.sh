#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_month=`date -d "30 day ago ${cur_date}" +%Y-%m-%d`
pre_haf_year=`date -d "180 day ago ${cur_date}" +%Y-%m-%d`

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" --conf "spark.sql.adaptive.shuffle.targetPostShuffleInputSize=128000000" --conf "spark.sql.adaptive.enabled=true" --conf "spark.app.name=dwb_vova_abnormal_low_price_goods" -e "

insert overwrite table dwb.dwb_vova_abnormal_low_price_goods  PARTITION (pt = '${cur_date}')
select c.virtual_goods_id,
       c.goods_name,
       a.avg_price,
       b.group_avg_price,
       a.gmv,
       d.mct_id,
       d.mct_name,
       b.group_avg_price - a.avg_price sheap,
       concat(round((b.group_avg_price - a.avg_price) * 100 / b.group_avg_price, 2), '%')  sheap_rate,
       e.lvyue,
       e.queren,
       e.tuotou,
       e.rate
from (

--圈定商品,平均售价
         select goods_id,
                sum(shop_price * goods_number + shipping_fee) / sum(goods_number) avg_price,
                sum(shop_price * goods_number + shipping_fee)                     gmv
         from dwd.dwd_vova_fact_pay
         where to_date(pay_time) >= '$pre_month'
           and datasource = 'vova'
         group by goods_id
     ) a
         join
     (
--同组商品平均售价
         select a.goods_id,
                a.group_number,
                b.group_avg_price
         from (
                  select a.goods_id,
                         max(a.group_number) group_number
                  from ads.ads_vova_min_price_goods_h a
                           join dim.dim_vova_order_goods b on a.goods_id = b.goods_id
                           join ods_vova_vts.ods_vova_order_goods_status e on b.order_goods_id = e.order_goods_id
                            left join dwd.dwd_vova_fact_refund d on b.order_goods_id = d.order_goods_id
                  where a.pt = '$cur_date'
                    and e.sku_shipping_status = 2
                    and (date_sub(to_date(d.create_time),10) > to_date(e.confirm_time) or d.order_goods_id is null)
                  group by a.goods_id
              ) a
                  left join (
             select a.group_number,
                    sum(b.shop_price * b.goods_number + b.shipping_fee) / sum(b.goods_number) group_avg_price
             from ads.ads_vova_min_price_goods_h a
                      join dim.dim_vova_order_goods b on a.goods_id = b.goods_id
                      join ods_vova_vts.ods_vova_order_goods_status e on b.order_goods_id = e.order_goods_id
                      left join dwd.dwd_vova_fact_refund d on b.order_goods_id = d.order_goods_id
             where a.pt = '$cur_date'
               and e.sku_shipping_status = 2
               and (date_sub(to_date(d.create_time),10) > to_date(e.confirm_time) or d.order_goods_id is null)
             group by group_number
         ) b on a.group_number = b.group_number
     ) b on a.goods_id = b.goods_id
         join dim.dim_vova_goods c on a.goods_id = c.goods_id
         left join dim.dim_vova_merchant d on c.mct_id = d.mct_id
         left join (
--最近半年履约订单
    select mct_id,
           sum(if((c.sku_shipping_status = 2 and d.order_goods_id is null) or (c.sku_shipping_status = 2 and date_sub(to_date(d.create_time),10) > to_date(c.confirm_time)), 1, 0)) lvyue,
           sum(if(a.sku_order_status >= 1, 1, 0))                                   queren,
           sum(if(c.sku_shipping_status = 2, 1, 0))                                 tuotou,
           round(sum(if(c.sku_shipping_status = 2 and d.refund_type_id = 2, 1, 0)) / sum(if(c.sku_shipping_status = 2, 1, 0)), 4)    rate
    from dim.dim_vova_order_goods a
             left join ods_vova_vts.ods_vova_order_goods_status c on a.order_goods_id = c.order_goods_id
             left join dwd.dwd_vova_fact_refund d on a.order_goods_id = d.order_goods_id
    where to_date(a.confirm_time) >= '$pre_haf_year'
    group by mct_id
) e on c.mct_id = e.mct_id
where a.avg_price < b.group_avg_price * 0.7
;
"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi



mysql -h rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pkkooxGjFy7Vgu21x <<EOF
drop table if exists backend.abnormal_low_price_goods_new;
drop table if exists backend.abnormal_low_price_goods_pre;

CREATE TABLE backend.abnormal_low_price_goods_new
(
    virtual_goods_id       bigint(20) NOT NULL COMMENT '虚拟id',
    goods_name             varchar(256) COMMENT '商品名称',
    avg_price              decimal(20, 2) COMMENT '商品均价',
    group_avg_price        decimal(20, 2) COMMENT '商品同组均价',
    gmv                    decimal(20, 2) DEFAULT '0' COMMENT '30天gmv',
    mct_id               bigint(20) COMMENT '店铺id',
    mct_name               varchar(64) COMMENT '店铺名称',
    diff_price             decimal(20, 2) COMMENT '商品便宜金额',
    diff_price_rate        varchar(32) COMMENT '商品便宜百分比',
    performance_order_num  bigint(20)     DEFAULT '0' COMMENT '履约订单数',
    confirm_order_num      bigint(20)     DEFAULT '0' COMMENT '已确认订单数',
    vote_order_num         bigint(20)     DEFAULT '0' COMMENT '已妥投订单数',
    vote_refund_order_rate varchar(32) COMMENT '妥投订单退款率',
    update_time            TIMESTAMP      DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP (0) COMMENT '更新时间',
    PRIMARY KEY (virtual_goods_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='异常低价取数';
EOF

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/backend \
--username bimaster --password kkooxGjFy7Vgu21x \
--table abnormal_low_price_goods_new \
--m 1 \
--hcatalog-database dwb \
--hcatalog-table dwb_vova_abnormal_low_price_goods \
--hcatalog-partition-keys pt \
--hcatalog-partition-values ${cur_date} \
--columns virtual_goods_id,goods_name,avg_price,group_avg_price,gmv,mct_id,mct_name,diff_price,diff_price_rate,performance_order_num,confirm_order_num,vote_order_num,vote_refund_order_rate \
--fields-terminated-by '\001'

if [ $? -ne 0 ];then
  exit 1
fi

mysql -h rec-backend.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u bimaster -pkkooxGjFy7Vgu21x <<EOF
rename table backend.abnormal_low_price_goods to backend.abnormal_low_price_goods_pre;
rename table backend.abnormal_low_price_goods_new to backend.abnormal_low_price_goods;
EOF

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
