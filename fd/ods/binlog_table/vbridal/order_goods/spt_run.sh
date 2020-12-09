#!/bin/sh
## 脚本参数注释:
## $1 表名【必传】
## $2 日期 %Y-%m-%d【非必传】

if [[ $# -lt 1 ]]; then
        echo "脚本必传一个参数，该参数代表是要执行的表名 【字符串类型】!"
        exit 1

elif [[ $# -ge 1 && $# -le 2 ]]; then
        echo $1 | grep "[a-zA-Z]" > /dev/null
        if [[ $? -eq 1 ]]; then
                echo "第一个参数[ $1 ]不符合要执行的表名, 请输入正确的表名!"
                exit 1
        fi
        table_name=$1
        pt=`date -d "-1 days" +%Y-%m-%d`
        pt_last=`date -d "-2 days" +%Y-%m-%d`

        if [[ $# -eq 2 ]]; then
                echo $2 | grep -Eq "[0-9]{4}-[0-9]{2}-[0-9]{2}" && date -d $2 +%Y-%m-%d > /dev/null
                if [[ $? -ne 0 ]]; then
                        echo "接收的第二个参数【${2}】不符合:%Y-%m-%d 时间个数，请输入正确的格式!"
                        exit 1
                fi
                table_name=$1
                pt=$2
                pt_last=`date -d "$2 -1 days" +%Y-%m-%d`
        fi
fi

#hive sql中使用的变量
echo $table_name
echo $pt
echo $pt_last

sql="
INSERT overwrite table ods_fd_vb.ods_fd_order_goods
select /*+ REPARTITION(10) */ rec_id, order_id, goods_style_id, sku, sku_id, goods_id, goods_name, goods_sn, goods_sku, goods_number, market_price, shop_price, shop_price_exchange, shop_price_amount_exchange, bonus, coupon_code, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, custom_fee, custom_fee_exchange, plussize_fee, plussize_fee_exchange, rush_order_fee, rush_order_fee_exchange, coupon_goods_id, coupon_cat_id, coupon_config_value, coupon_config_coupon_type, styles, img_type, goods_gallery, goods_price_original, wrap_price, wrap_price_exchange, display_shop_price_exchange, display_shop_price_amount_exchange, display_custom_fee_exchange, display_plussize_fee_exchange, display_rush_order_fee_exchange, display_wrap_price_exchange, heel_type_price, heel_type_price_exchange, display_heel_type_price_exchange
from ods_fd_vb.ods_fd_order_goods_arc
where  pt='$pt';
"
#脚本路径
shell_path="/mnt/vova-bigdata-scripts/fd/ods/binlog_table/vbridal"

#snapshot表
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=fd_order_goods_snapshot_gaohaitao"   --conf "spark.sql.output.coalesceNum=40" --conf "spark.dynamicAllocation.minExecutors=40" --conf "spark.dynamicAllocation.initialExecutors=60" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
echo "step: ${table_name}_snapshot table is finished !"
