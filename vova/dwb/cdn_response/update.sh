#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

#TEST
##dependence
#dwd.dwd_vova_log_data
sql="
insert overwrite table tmp.tmp_dwb_vova_app_response
SELECT
/*+ REPARTITION(1) */
geo_country,
CASE WHEN url_path LIKE '%image-tb.vova.com%' THEN 'img'
     WHEN url_path LIKE '%d2bq6dn4rcskkx.cloudfront.net%' THEN 'img'
     WHEN url_path LIKE '%image.vova.com%' THEN 'resource'
     WHEN url_path LIKE '%d17qpog9ccnbkb.cloudfront.net%' THEN 'resource'
     WHEN url_path LIKE '%/v3/surface/index%' THEN 'api'
ELSE 'others' END resource_type,
res_time,
pt
FROM (
         SELECT get_json_object(extra, '$.path') AS url_path,
                get_json_object(extra, '$.time') AS res_time,
                geo_country,
                device_id,
                pt,
                element_name
         FROM dwd.dwd_vova_log_data log
         WHERE log.pt = '${cur_date}'
     ) temp1
WHERE temp1.url_path is not null
AND temp1.res_time > 0;


-- table1
INSERT OVERWRITE TABLE dwb.dwb_vova_app_response PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
'${cur_date}' as event_date,
region_code,
resource_type,
res_100,
res_200,
res_300,
res_400,
res_500,
res_600,
res_700,
res_800,
res_900,
res_1000,
res_1100,
res_1200,
res_1300,
res_1400,
res_1500,
res_1600,
res_1700,
res_1800,
res_1900,
res_2000,
res_2100,
res_2200,
res_2300,
res_2400,
res_2500,
res_2600,
res_2700,
res_2800,
res_2900,
res_3000,
res_3100,
res_total,
concat(nvl(round(res_100 / res_total * 100,2), 0), '%') as res_rate_100,
concat(nvl(round(res_200 / res_total * 100,2), 0), '%') as res_rate_200,
concat(nvl(round(res_300 / res_total * 100,2), 0), '%') as res_rate_300,
concat(nvl(round(res_400 / res_total * 100,2), 0), '%') as res_rate_400,
concat(nvl(round(res_500 / res_total * 100,2), 0), '%') as res_rate_500,
concat(nvl(round(res_600 / res_total * 100,2), 0), '%') as res_rate_600,
concat(nvl(round(res_700 / res_total * 100,2), 0), '%') as res_rate_700,
concat(nvl(round(res_800 / res_total * 100,2), 0), '%') as res_rate_800,
concat(nvl(round(res_900 / res_total * 100,2), 0), '%') as res_rate_900,
concat(nvl(round(res_1000 / res_total * 100,2), 0), '%') as res_rate_1000,
concat(nvl(round(res_1100 / res_total * 100,2), 0), '%') as res_rate_1100,
concat(nvl(round(res_1200 / res_total * 100,2), 0), '%') as res_rate_1200,
concat(nvl(round(res_1300 / res_total * 100,2), 0), '%') as res_rate_1300,
concat(nvl(round(res_1400 / res_total * 100,2), 0), '%') as res_rate_1400,
concat(nvl(round(res_1500 / res_total * 100,2), 0), '%') as res_rate_1500,
concat(nvl(round(res_1600 / res_total * 100,2), 0), '%') as res_rate_1600,
concat(nvl(round(res_1700 / res_total * 100,2), 0), '%') as res_rate_1700,
concat(nvl(round(res_1800 / res_total * 100,2), 0), '%') as res_rate_1800,
concat(nvl(round(res_1900 / res_total * 100,2), 0), '%') as res_rate_1900,
concat(nvl(round(res_2000 / res_total * 100,2), 0), '%') as res_rate_2000,
concat(nvl(round(res_2100 / res_total * 100,2), 0), '%') as res_rate_2100,
concat(nvl(round(res_2200 / res_total * 100,2), 0), '%') as res_rate_2200,
concat(nvl(round(res_2300 / res_total * 100,2), 0), '%') as res_rate_2300,
concat(nvl(round(res_2400 / res_total * 100,2), 0), '%') as res_rate_2400,
concat(nvl(round(res_2500 / res_total * 100,2), 0), '%') as res_rate_2500,
concat(nvl(round(res_2600 / res_total * 100,2), 0), '%') as res_rate_2600,
concat(nvl(round(res_2700 / res_total * 100,2), 0), '%') as res_rate_2700,
concat(nvl(round(res_2800 / res_total * 100,2), 0), '%') as res_rate_2800,
concat(nvl(round(res_2900 / res_total * 100,2), 0), '%') as res_rate_2900,
concat(nvl(round(res_3000 / res_total * 100,2), 0), '%') as res_rate_3000,
concat(nvl(round(res_3100 / res_total * 100,2), 0), '%') as res_rate_3100
FROM
(
SELECT nvl(nvl(geo_country,'NALL'),'all') as region_code,
       nvl(resource_type,'all') as resource_type,
       sum(t1) AS res_100,
       sum(t2) AS res_200,
       sum(t3) AS res_300,
       sum(t4) AS res_400,
       sum(t5) AS res_500,
       sum(t6) AS res_600,
       sum(t7) AS res_700,
       sum(t8) AS res_800,
       sum(t9) AS res_900,
       sum(t10) AS res_1000,
       sum(t11) AS res_1100,
       sum(t12) AS res_1200,
       sum(t13) AS res_1300,
       sum(t14) AS res_1400,
       sum(t15) AS res_1500,
       sum(t16) AS res_1600,
       sum(t17) AS res_1700,
       sum(t18) AS res_1800,
       sum(t19) AS res_1900,
       sum(t20) AS res_2000,
       sum(t21) AS res_2100,
       sum(t22) AS res_2200,
       sum(t23) AS res_2300,
       sum(t24) AS res_2400,
       sum(t25) AS res_2500,
       sum(t26) AS res_2600,
       sum(t27) AS res_2700,
       sum(t28) AS res_2800,
       sum(t29) AS res_2900,
       sum(t30) AS res_3000,
       sum(t31) AS res_3100,
       count(*) as res_total
FROM (
         SELECT geo_country,
                pt,
                resource_type,
                if(res_time >= 0 AND res_time <= 100, 1, 0)    AS t1,
                if(res_time > 100 AND res_time <= 200, 1, 0)   AS t2,
                if(res_time > 200 AND res_time <= 300, 1, 0)   AS t3,
                if(res_time > 300 AND res_time <= 400, 1, 0)   AS t4,
                if(res_time > 400 AND res_time <= 500, 1, 0)   AS t5,
                if(res_time > 500 AND res_time <= 600, 1, 0)   AS t6,
                if(res_time > 600 AND res_time <= 700, 1, 0)   AS t7,
                if(res_time > 700 AND res_time <= 800, 1, 0)   AS t8,
                if(res_time > 800 AND res_time <= 900, 1, 0)   AS t9,
                if(res_time > 900 AND res_time <= 1000, 1, 0)  AS t10,
                if(res_time > 1000 AND res_time <= 1100, 1, 0) AS t11,
                if(res_time > 1100 AND res_time <= 1200, 1, 0) AS t12,
                if(res_time > 1200 AND res_time <= 1300, 1, 0) AS t13,
                if(res_time > 1300 AND res_time <= 1400, 1, 0) AS t14,
                if(res_time > 1400 AND res_time <= 1500, 1, 0) AS t15,
                if(res_time > 1500 AND res_time <= 1600, 1, 0) AS t16,
                if(res_time > 1600 AND res_time <= 1700, 1, 0) AS t17,
                if(res_time > 1700 AND res_time <= 1800, 1, 0) AS t18,
                if(res_time > 1800 AND res_time <= 1900, 1, 0) AS t19,
                if(res_time > 1900 AND res_time <= 2000, 1, 0) AS t20,
                if(res_time > 2000 AND res_time <= 2100, 1, 0) AS t21,
                if(res_time > 2100 AND res_time <= 2200, 1, 0) AS t22,
                if(res_time > 2200 AND res_time <= 2300, 1, 0) AS t23,
                if(res_time > 2300 AND res_time <= 2400, 1, 0) AS t24,
                if(res_time > 2400 AND res_time <= 2500, 1, 0) AS t25,
                if(res_time > 2500 AND res_time <= 2600, 1, 0) AS t26,
                if(res_time > 2600 AND res_time <= 2700, 1, 0) AS t27,
                if(res_time > 2700 AND res_time <= 2800, 1, 0) AS t28,
                if(res_time > 2800 AND res_time <= 2900, 1, 0) AS t29,
                if(res_time > 2900 AND res_time <= 3000, 1, 0) AS t30,
                if(res_time > 3000, 1, 0)                      AS t31,
                res_time
         FROM tmp.tmp_dwb_vova_app_response
         WHERE pt = '${cur_date}'
     ) temp1
GROUP BY CUBE (nvl(geo_country,'NALL'),resource_type)
) final;


-- temp table2
insert overwrite table tmp.tmp_dwb_vova_app_response2
SELECT
/*+ REPARTITION(1) */
geo_country,
resource_type,
res_time,
pt
FROM
tmp.tmp_dwb_vova_app_response
WHERE pt = '${cur_date}'
UNION ALL
SELECT
'all' as geo_country,
'all' as resource_type,
res_time,
pt
FROM
tmp.tmp_dwb_vova_app_response
WHERE pt = '${cur_date}'
UNION ALL
SELECT
'all' as geo_country,
resource_type,
res_time,
pt
FROM
tmp.tmp_dwb_vova_app_response
WHERE pt = '${cur_date}'
UNION ALL
SELECT
geo_country,
'all' as resource_type,
res_time,
pt
FROM
tmp.tmp_dwb_vova_app_response
WHERE pt = '${cur_date}';


drop table if exists tmp.tmp_dwb_vova_app_response_avg;
CREATE TABLE tmp.tmp_dwb_vova_app_response_avg AS
--  asc data
select
'${cur_date}' as event_date,
'asc_avg100' as top_split,
avg(res_time) as res_time,
geo_country as region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response2
group by geo_country, resource_type

UNION

select
'${cur_date}' as event_date,
'asc_tp100' as top_split,
max(res_time) as res_time,
geo_country as region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response2
group by geo_country, resource_type

UNION

select
'${cur_date}' as event_date,
'asc_avg50' as top_split,
avg(res_time) as res_time,
geo_country as region_code,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time ASC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)<=50
group by geo_country, resource_type

UNION

select
'${cur_date}' as event_date,
'asc_avg90' as top_split,
avg(res_time) as res_time,
geo_country as region_code,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time ASC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)<=90
group by geo_country, resource_type

UNION

select
'${cur_date}' as event_date,
'asc_avg95' as top_split,
avg(res_time) as res_time,
geo_country as region_code,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time ASC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)<=95
group by geo_country, resource_type

UNION

select
pt,
'asc_tp50' as top_split,
res_time,
geo_country,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time ASC) as rank2
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time ASC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)>=50
) temp2
where rank2 = 1

UNION

select
pt,
'asc_tp90' as top_split,
res_time,
geo_country,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time ASC) as rank2
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time ASC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)>=90
) temp2
where rank2 = 1

UNION

select
pt,
'asc_tp95' as top_split,
res_time,
geo_country,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time ASC) as rank2
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time ASC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)>=95
) temp2
where rank2 = 1

UNION
-- desc data

select
'${cur_date}' as event_date,
'desc_tp100' as top_split,
min(res_time) as res_time,
geo_country as region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response2
group by geo_country, resource_type

UNION

select
'${cur_date}' as event_date,
'desc_avg50' as top_split,
avg(res_time) as res_time,
geo_country as region_code,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time DESC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)<=50
group by geo_country, resource_type

UNION

select
'${cur_date}' as event_date,
'desc_avg90' as top_split,
avg(res_time) as res_time,
geo_country as region_code,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time DESC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)<=90
group by geo_country, resource_type

UNION

select
'${cur_date}' as event_date,
'desc_avg95' as top_split,
avg(res_time) as res_time,
geo_country as region_code,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time DESC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)<=95
group by geo_country, resource_type

UNION

select
pt,
'desc_tp90' as top_split,
res_time,
geo_country,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time DESC) as rank2
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time DESC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)>=90
) temp2
where rank2 = 1

UNION

select
pt,
'desc_tp95' as top_split,
res_time,
geo_country,
resource_type
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time DESC) as rank2
from
(
select
pt,
res_time,
geo_country,
resource_type,
row_number() over (partition by pt,resource_type,geo_country order by res_time DESC) as rank1,
count(*) over(partition by pt,resource_type,geo_country) tot
from
tmp.tmp_dwb_vova_app_response2
WHERE pt = '${cur_date}'
) temp1
where round(100 * rank1/tot)>=95
) temp2
where rank2 = 1;

-- final
INSERT OVERWRITE TABLE dwb.dwb_vova_app_response_top PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
asc_avg100.event_date,
asc_avg100.region_code,
asc_avg100.resource_type,
asc_tp50.asc_tp50,
asc_tp90.asc_tp90,
asc_tp95.asc_tp95,
asc_tp100.asc_tp100,
asc_avg50.asc_avg50,
asc_avg90.asc_avg90,
asc_avg95.asc_avg95,
asc_avg100.asc_avg100,
desc_tp90.desc_tp90,
desc_tp95.desc_tp95,
desc_tp100.desc_tp100,
desc_avg50.desc_avg50,
desc_avg90.desc_avg90,
desc_avg95.desc_avg95
from
(
select
event_date,
res_time as asc_avg100,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'asc_avg100'
and event_date = '${cur_date}'
) asc_avg100
left join
(
select
event_date,
res_time as asc_avg50,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'asc_avg50'
and event_date = '${cur_date}'
) asc_avg50 ON asc_avg100.region_code = asc_avg50.region_code
and asc_avg100.resource_type = asc_avg50.resource_type
left join
(
select
event_date,
res_time as asc_avg90,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'asc_avg90'
and event_date = '${cur_date}'
) asc_avg90 ON asc_avg100.region_code = asc_avg90.region_code
and asc_avg100.resource_type = asc_avg90.resource_type

left join
(
select
event_date,
res_time as asc_avg95,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'asc_avg95'
and event_date = '${cur_date}'
) asc_avg95 ON asc_avg100.region_code = asc_avg95.region_code
and asc_avg100.resource_type = asc_avg95.resource_type

left join
(
select
event_date,
res_time as asc_tp50,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'asc_tp50'
and event_date = '${cur_date}'
) asc_tp50 ON asc_avg100.region_code = asc_tp50.region_code
and asc_avg100.resource_type = asc_tp50.resource_type

left join
(
select
event_date,
res_time as asc_tp90,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'asc_tp90'
and event_date = '${cur_date}'
) asc_tp90 ON asc_avg100.region_code = asc_tp90.region_code
and asc_avg100.resource_type = asc_tp90.resource_type
left join
(
select
event_date,
res_time as asc_tp95,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'asc_tp95'
and event_date = '${cur_date}'
) asc_tp95 ON asc_avg100.region_code = asc_tp95.region_code
and asc_avg100.resource_type = asc_tp95.resource_type

left join
(
select
event_date,
res_time as asc_tp100,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'asc_tp100'
and event_date = '${cur_date}'
) asc_tp100 ON asc_avg100.region_code = asc_tp100.region_code
and asc_avg100.resource_type = asc_tp100.resource_type

left join
(
select
event_date,
res_time as desc_avg50,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'desc_avg50'
and event_date = '${cur_date}'
) desc_avg50 ON asc_avg100.region_code = desc_avg50.region_code
and asc_avg100.resource_type = desc_avg50.resource_type
left join
(
select
event_date,
res_time as desc_avg90,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'desc_avg90'
and event_date = '${cur_date}'
) desc_avg90 ON asc_avg100.region_code = desc_avg90.region_code
and asc_avg100.resource_type = desc_avg90.resource_type

left join
(
select
event_date,
res_time as desc_avg95,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'desc_avg95'
and event_date = '${cur_date}'
) desc_avg95 ON asc_avg100.region_code = desc_avg95.region_code
and asc_avg100.resource_type = desc_avg95.resource_type

left join
(
select
event_date,
res_time as desc_tp90,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'desc_tp90'
and event_date = '${cur_date}'
) desc_tp90 ON asc_avg100.region_code = desc_tp90.region_code
and asc_avg100.resource_type = desc_tp90.resource_type
left join
(
select
event_date,
res_time as desc_tp95,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'desc_tp95'
and event_date = '${cur_date}'
) desc_tp95 ON asc_avg100.region_code = desc_tp95.region_code
and asc_avg100.resource_type = desc_tp95.resource_type

left join
(
select
event_date,
res_time as desc_tp100,
region_code,
resource_type
from
tmp.tmp_dwb_vova_app_response_avg t1
where region_code is not null
and top_split = 'desc_tp100'
and event_date = '${cur_date}'
) desc_tp100 ON asc_avg100.region_code = desc_tp100.region_code
and asc_avg100.resource_type = desc_tp100.resource_type


;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=app_response" -e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi