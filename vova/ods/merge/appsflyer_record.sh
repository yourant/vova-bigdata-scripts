#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###更新用户首单
sql="
insert overwrite table ods_vova_vtlr.ods_vova_appsflyer_record_merge
select
/*+ REPARTITION(100) */
datasource,
device_id,
media_source,
platform,
idfv,
android_id,
imei,
advertising_id,
http_referrer,
campaign,
os_version,
country_code,
language,
install_time,
app_version,
bundle_id,
create_time
from
(
select
datasource,
device_id,
media_source,
platform,
idfv,
android_id,
imei,
advertising_id,
http_referrer,
campaign,
os_version,
country_code,
language,
install_time,
app_version,
bundle_id,
create_time,
row_number () OVER (PARTITION BY datasource,device_id ORDER BY create_time) AS rank
from
(
select 'vova' datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,NULL bundle_id,create_time from ods_vova_vtl.ods_vova_appsflyer_record
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_0
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_1
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_2
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_3
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_4
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_5
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_6
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_7
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_8
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_9
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_10
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_11
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_12
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_13
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_14
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_15
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_16
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_17
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_18
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_19
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_20
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_21
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_22
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_23
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_24
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_25
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_26
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_27
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_28
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_29
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_30
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_31
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_32
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_33
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_34
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_35
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_36
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_37
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_38
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_39
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_40
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_41
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_42
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_43
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_44
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_45
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_46
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_47
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_48
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_49
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_50
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_51
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_52
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_53
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_54
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_55
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_56
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_57
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_58
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_59
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_60
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_61
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_62
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_vova_vtlr.ods_vova_appsflyer_record_new_63
union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_0
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_1
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_2
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_3
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_4
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_5
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_6
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_7
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_8
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_9
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_10
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_11
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_12
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_13
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_14
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_15
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_16
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_17
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_18
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_19
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_20
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_21
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_22
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_23
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_24
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_25
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_26
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_27
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_28
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_29
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_30
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_31
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_32
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_33
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_34
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_35
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_36
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_37
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_38
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_39
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_40
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_41
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_42
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_43
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_44
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_45
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_46
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_47
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_48
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_49
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_50
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_51
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_52
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_53
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_54
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_55
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_56
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_57
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_58
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_59
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_60
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_61
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_62
 union all
select split(bundle_id,'\\\.')[1] datasource,device_id,media_source,platform,idfv,android_id,imei,advertising_id,http_referrer,campaign,os_version,country_code,language,install_time,app_version,bundle_id,create_time from ods_ac_aclr.ods_ac_appsflyer_record_new_63
) t
) t where rank =1;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.app.name=merge_appsflyer_record" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
