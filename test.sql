spark-submit \
--deploy-mode client \
--name ''dwb_ad_income_order_email'' \
--master yarn  \
--conf spark.executor.memory=4g \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.executor.memoryOverhead=2048 \
--class com.vova.utils.EmailUtil s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
-sql "select cat_name,
day_value,
day_rate,
month_value,
month_rate,
case
--when cat_name=concat("<p style=text-align: left\;height: 1px\;line-height: 1px\;>",''总订单数'',''</p>'') then ''a''
when cat_name=''退货/退款订单数'' then ''b''
when cat_name=''净销售订单数'' then ''c=a-b''
when cat_name=''商品销售总金额'' then ''d''
when cat_name=''退款金额'' then ''e''
when cat_name=''商品销售净收入'' then ''h=d-e''
when cat_name=''红包消耗总金额'' then ''i=j+k''
when cat_name=''发货红包金额'' then ''j''
when cat_name=''退货红包金额'' then ''i''
when cat_name=''物流费盈亏'' then ''l=m+n''
when cat_name=''代收快递费'' then ''m''
when cat_name=''代付快递费'' then ''n''
when cat_name=''商品采购成本'' then ''p''
when cat_name=''广告费'' then ''q''
when cat_name=''支付手续费'' then ''r''
when cat_name=''毛利'' then ''s=h+i+l-p-q-r''
when cat_name=''毛利率%'' then ''t=s/h''
when cat_name=''总费用'' then ''u=v+w+x+y''
when cat_name=''人员工资'' then ''v''
when cat_name=''仓储费'' then ''w''
when cat_name=''房租'' then ''x''
when cat_name=''服务器费用'' then ''y''
when cat_name=''净利润'' then ''z=s-u''
when cat_name=''净利率'' then ''(s-u)/d''
end as c
from
dwb.dwb_ad_income_order_1
where pt=''2021-07-15''
order by rn"  \
-head "科目,2021-07-15,占比（取绝对值）,本月截止昨日累计,占比（取绝对值）,科目编号&公式"  \
-receiver "bob.zhu@i9i8.com" \
-title "AD利润报表(订单维度,2021-07-15)"
