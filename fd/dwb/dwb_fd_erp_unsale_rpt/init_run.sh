#!/bin/sh

if [ ! -n "$1" ] ;then
    pt=`date -d "-1 days" +%Y-%m-%d`
    pt3=`date -d "-4 days" +%Y-%m-%d`
    pt11=`date -d "-12 days" +%Y-%m-%d`
else
    echo $1 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $1 +%Y-%m-%d > /dev/null
    if [[ $? -ne 0 ]]; then
        echo "接收的时间格式${1}不符合:%Y-%m-%d，请输入正确的格式!"
        exit
    fi
    pt=$1
    pt3=`date -d "$1 -3 days" +%Y-%m-%d`
    pt11=`date -d "$1 -11 days" +%Y-%m-%d`

fi

#hive sql中使用的变量
echo $pt
echo $pt3
echo $pt11

shell_path="/mnt/vova-bigdata-scripts/fd/dwb/dwb_fd_erp_unsale_rpt"

#最近14天每天平均销量
spark-sql \
  --conf "spark.app.name=erp_14d_avg_sale_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=10" \
  -d pt=$pt \
  -d pt3=$pt3 \
  -d pt11=$pt11 \
  -f ${shell_path}/erp_14d_avg_sale.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "erp_14d_avg_sale table is finished !"

#月销售数
spark-sql \
  --conf "spark.app.name=erp_goods_sale_monthly_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=40" \
  -d pt=$pt \
  -d pt3=$pt3 \
  -d pt11=$pt11 \
  -f ${shell_path}/erp_goods_sale_monthly.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "erp_14d_avg_sale table is finished !"

#goods_sku
spark-sql \
  --conf "spark.app.name=erp_goods_sku_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=10" \
  -d pt=$pt \
  -d pt3=$pt3 \
  -d pt11=$pt11 \
  -f ${shell_path}/erp_goods_sku.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "erp_goods_sku table is finished !"

#备货天数指标数据
spark-sql \
  --conf "spark.app.name=erp_goods_stock_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=10" \
  -d pt=$pt \
  -d pt3=$pt3 \
  -d pt11=$pt11 \
  -f ${shell_path}/erp_goods_stock.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "erp_goods_stock table is finished !"

#可预订库存
spark-sql \
  --conf "spark.app.name=erp_reserve_goods_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=30" \
  -d pt=$pt \
  -d pt3=$pt3 \
  -d pt11=$pt11 \
  -f ${shell_path}/erp_reserve_goods.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "erp_reserve_goods table is finished !"

#未预定上的订单需求数
spark-sql \
  --conf "spark.app.name=erp_unreserve_order.hql_gaohaitao" \
  --conf "spark.dynamicAllocation.maxExecutors=30" \
  -d pt=$pt \
  -d pt3=$pt3 \
  -d pt11=$pt11 \
  -f ${shell_path}/erp_unreserve_order.hql

if [ $? -ne 0 ]; then
  exit 1
fi
echo "erp_unreserve_order.hql table is finished !"