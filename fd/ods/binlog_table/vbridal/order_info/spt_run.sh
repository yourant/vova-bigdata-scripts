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
INSERT overwrite table ods_fd_vb.ods_fd_order_info
select /*+ REPARTITION(10) */ order_id , party_id , order_sn , user_id , order_time , order_status , shipping_status , pay_status , consignee , gender , country , province , province_text , city , city_text , district , district_text , address , zipcode , tel , mobile , email , best_time , sign_building , postscript , important_day , sm_id , shipping_id , shipping_name , payment_id , payment_name , how_oos , how_surplus , pack_name , card_name , card_message , inv_payee , inv_content , inv_address , inv_zipcode , inv_phone , goods_amount , goods_amount_exchange , shipping_fee , duty_fee , shipping_fee_exchange , duty_fee_exchange , insure_fee , shipping_proxy_fee , payment_fee , pack_fee , card_fee , money_paid , surplus , integral , integral_money , bonus , bonus_exchange , order_amount , base_currency_id , order_currency_id , order_currency_symbol , rate , order_amount_exchange , from_ad , referer , confirm_time , pay_time , shipping_time , shipping_date_estimate , shipping_carrier , shipping_tracking_number , pack_id , card_id , coupon_code , invoice_no , extension_code , extension_id , to_buyer , pay_note , invoice_status , carrier_bill_id , receiving_time , biaoju_store_id , parent_order_id , track_id , ga_track_id , real_paid , real_shipping_fee , is_shipping_fee_clear , is_order_amount_clear , is_ship_emailed , proxy_amount , pay_method , is_back , is_finance_clear , finance_clear_type , handle_time , start_shipping_time , end_shipping_time , shortage_status , is_shortage_await , order_type_id , special_type_id , is_display , misc_fee , additional_amount , distributor_id , taobao_order_sn , distribution_purchase_order_sn , need_invoice , facility_id , language_id , coupon_cat_id , coupon_config_value , coupon_config_coupon_type , is_conversion , from_domain , project_name , user_agent_id , display_currency_id , display_currency_rate , display_shipping_fee_exchange , display_duty_fee_exchange , display_order_amount_exchange , display_goods_amount_exchange , display_bonus_exchange , token , payer_id
from ods_fd_vb.ods_fd_order_info_arc where pt = '$pt';
"

#snapshot表
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.app.name=fd_order_info_snapshot_gaohaitao"   --conf "spark.sql.output.coalesceNum=40" --conf "spark.dynamicAllocation.minExecutors=40" --conf "spark.dynamicAllocation.initialExecutors=60" -e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi
echo "step: ${table_name}_snapshot table is finished !"
