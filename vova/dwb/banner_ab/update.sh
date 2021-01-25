#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-sql   --conf "spark.app.name=dwb_vova_banner_ab" --conf "spark.sql.crossJoin.enabled=true"  --conf "spark.dynamicAllocation.maxExecutors=100"  -e "
insert overwrite table dwb.dwb_vova_banner_ab PARTITION (pt = '${cur_date}')
select
       a.pt,a.geo_country,a.app_version,a.rec_version, a.is_gods, nvl(a.expre_pv,0),nvl(b.clk_pv,0),concat(round(b.clk_pv * 100 / a.expre_pv,2),'%') ctr,
       nvl(a.expre_uv,0),nvl(b.clk_uv,0),concat(round(b.clk_uv * 100 / a.expre_uv,2),'%') ctr_uv
       from
(
    select nvl(app_version,'all') app_version,nvl(pt,'all') pt,nvl(geo_country,'all') geo_country,
           nvl(split(ab_test, '_')[size(split(ab_test, '_')) - 1],'all') rec_version,
           nvl(if(length(element_id) in (7,8), 'Y', 'N'),'all') is_gods,
           count(*) expre_pv,
           count(distinct device_id) expre_uv
    from (
             select t.app_version,t.geo_country,pt,
                    t.element_id,t.device_id,
                    ab_test
             from (
                      select app_version,pt,geo_country,device_id,
                             element_id,split(test_info, '&')       test_info
                      from dwd.dwd_vova_log_impressions
                      where pt = '${cur_date}' and page_code = 'homepage'
                        and element_name like '%Banner_outlet%'
                        and geo_country in ('FR','GB','ES','IT')
                  ) t LATERAL VIEW explode(t.test_info) ab_tes as ab_test
         ) tmp
    where substr(ab_test, 0, length(ab_test) - length(split(ab_test, '_')[size(split(ab_test, '_')) - 1]) - 1) =
          'rec_banner'
    group by cube (pt,geo_country,app_version, split(ab_test, '_')[size(split(ab_test, '_')) - 1], if(length(element_id) in (7,8), 'Y', 'N'))
) a
left join (

    select nvl(app_version,'all') app_version,nvl(pt,'all') pt,nvl(geo_country,'all') geo_country,
           nvl(split(ab_test, '_')[size(split(ab_test, '_')) - 1],'all') rec_version,
           nvl(if(length(element_id) in (7,8), 'Y', 'N'),'all') is_gods,
           count(*) clk_pv,
           count(distinct device_id) clk_uv
    from (
             select t.app_version,pt,geo_country,
                    t.element_id,t.device_id,
                    ab_test
             from (
                      select app_version,pt,geo_country,device_id,
                             element_id,split(test_info, '&')       test_info
                      from dwd.dwd_vova_log_common_click
                      where pt = '${cur_date}' and page_code = 'homepage'
                        and element_name like '%Banner_outlet%'
                        and geo_country in ('FR','GB','ES','IT')
                  ) t LATERAL VIEW explode(t.test_info) ab_tes as ab_test
         ) tmp
    where substr(ab_test, 0, length(ab_test) - length(split(ab_test, '_')[size(split(ab_test, '_')) - 1]) - 1) =
          'rec_banner'
    group by cube (pt,geo_country,app_version, split(ab_test, '_')[size(split(ab_test, '_')) - 1], if(length(element_id) in (7,8), 'Y', 'N'))

) b
on a.app_version = b.app_version
and a.rec_version = b.rec_version
and a.is_gods = b.is_gods
and a.pt = b.pt
and a.geo_country = b.geo_country
;
"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
