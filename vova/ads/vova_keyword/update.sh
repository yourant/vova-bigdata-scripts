#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  cur_date=$(date -d "-1 day" +%Y-%m-%d)
fi

sql="
add jar hdfs:///tmp/jar/base64_to_long_udtf.jar;
CREATE TEMPORARY FUNCTION Base64ToLongUDTF as 'com.vova.bigdata.sparkbatch.utils.Base64ToLongUDTF';
with tmp_score as (
select goods_id from mlb.mlb_vova_rec_b_goods_score_d where pt='${cur_date}' and overall_score>=30
),

tmp_usable as (
select goods_id,t1.attr_value,second_cat_id,first_cat_id,third_cat_id,fourth_cat_id from ads.ads_vova_goods_attribute_merge t1
inner join (select attr_value from ads.ads_vova_usable_value) t2 on t1.attr_value = t2.attr_value
where t1.pt = '${cur_date}'
),

tmp_brand as (
select a.goods_id,b.brand_name as keyword,2 as weight from dim.dim_vova_goods a
inner join tmp_score s on a.goods_id=s.goods_id
left join ods_vova_vts.ods_vova_brand b on a.brand_id=b.brand_id
where a.brand_id>0 and a.is_on_sale=1
),

tmp_cat as (
select distinct a.goods_id,keyword,1 as weight from (
select goods_id,second_cat_name as keyword from dim.dim_vova_goods where second_cat_name is not null and is_on_sale=1
union all
select goods_id,third_cat_name as keyword from dim.dim_vova_goods where third_cat_name is not null and is_on_sale=1
union all
select goods_id,fourth_cat_name as keyword from dim.dim_vova_goods where fourth_cat_name is not null and is_on_sale=1
) a inner join tmp_score s on a.goods_id=s.goods_id
),

tmp_value_cat as (
	select distinct a.goods_id,keyword,3 as weight from (
		select goods_id,concat(attr_value,' ',b.second_cat_name) as keyword from tmp_usable a
		left join (select distinct second_cat_id,second_cat_name from dim.dim_vova_category) b on a.second_cat_id=b.second_cat_id
		where a.second_cat_id is not null
		union all
		select goods_id,concat(attr_value,' ',b.three_cat_name) as keyword from tmp_usable a
		left join (select distinct three_cat_id,three_cat_name from dim.dim_vova_category) b on a.third_cat_id=b.three_cat_id
		where a.third_cat_id is not null
		union all
		select goods_id,concat(attr_value,' ',b.four_cat_name) as keyword from tmp_usable a
		left join (select distinct four_cat_id,four_cat_name from dim.dim_vova_category) b on a.fourth_cat_id=b.four_cat_id
		where a.fourth_cat_id is not null
	) a inner join tmp_score s on a.goods_id=s.goods_id
)

insert overwrite table ads.ads_vova_result_page_keyword_rank_data PARTITION (pt = '${cur_date}')
select 1 as type,goods_id as type_value,keyword_id,row_number() over(partition by goods_id order by weight desc) as rank
from (
	select goods_id, t2.keyword_id,max(weight) as weight from (
	select goods_id,initcap(keyword) as keyword,weight from tmp_brand
	union all
	select goods_id,initcap(keyword) as keyword,weight from tmp_cat
	union all
	select goods_id,initcap(keyword) as keyword,weight from tmp_value_cat
	) t1 inner join ads.ads_vova_keyword t2 on t1.keyword=t2.keyword_name group by goods_id,t2.keyword_id
);


with tmp_score as (
select goods_id,overall_score from mlb.mlb_vova_rec_b_goods_score_d where pt='${cur_date}'
),

tmp_cat as (
	select goods_id,cat_id,cat_name from (
		select t1.goods_id,t1.cat_id,t1.cat_name,row_number() over(partition by t1.cat_id order by s.overall_score desc) as rank from (
			select distinct a.goods_id,cat_id,cat_name from (
			select goods_id,second_cat_id as cat_id,second_cat_name as cat_name from dim.dim_vova_goods where second_cat_id is not null and is_on_sale=1
			union all
			select goods_id,third_cat_id as cat_id,third_cat_name as cat_name  from dim.dim_vova_goods where third_cat_id is not null and is_on_sale=1
			union all
			select goods_id,fourth_cat_id as cat_id,fourth_cat_name as cat_name  from dim.dim_vova_goods where fourth_cat_id is not null and is_on_sale=1
			) a
		) t1 inner join tmp_score s on t1.goods_id=s.goods_id
	) T where rank<=1000
),

tmp_cat_keyword as (
	select distinct a.goods_id,keyword from (
	select goods_id,second_cat_name as keyword from dim.dim_vova_goods where second_cat_name is not null and is_on_sale=1
	union all
	select goods_id,third_cat_name as keyword from dim.dim_vova_goods where third_cat_name is not null and is_on_sale=1
	union all
	select goods_id,fourth_cat_name as keyword from dim.dim_vova_goods where fourth_cat_name is not null and is_on_sale=1
	) a
),

tmp_usable as (
select goods_id,t1.attr_value,second_cat_id,first_cat_id,third_cat_id,fourth_cat_id from ads.ads_vova_goods_attribute_merge t1
inner join (select * from ads.ads_vova_usable_value) t2 on t1.attr_value = t2.attr_value
where t1.pt = '${cur_date}'
),

tmp_value_cat as (
	select distinct a.goods_id,keyword,cat_name from (
		select goods_id,concat(attr_value,' ',b.second_cat_name) as keyword,b.second_cat_name as cat_name from tmp_usable a
		left join (select distinct second_cat_id,second_cat_name from dim.dim_vova_category) b on a.second_cat_id=b.second_cat_id
		where a.second_cat_id is not null
		union all
		select goods_id,concat(attr_value,' ',b.three_cat_name) as keyword,b.three_cat_name as cat_name from tmp_usable a
		left join (select distinct three_cat_id,three_cat_name from dim.dim_vova_category) b on a.third_cat_id=b.three_cat_id
		where a.third_cat_id is not null
		union all
		select goods_id,concat(attr_value,' ',b.four_cat_name) as keyword,b.four_cat_name as cat_name from tmp_usable a
		left join (select distinct four_cat_id,four_cat_name from dim.dim_vova_category) b on a.fourth_cat_id=b.four_cat_id
		where a.fourth_cat_id is not null
	) a
)

insert into table ads.ads_vova_result_page_keyword_rank_data PARTITION (pt = '${cur_date}')
	select
	2 as type,cat_id as type_value,keyword_id,row_number() over(partition by cat_id order by num desc) as rank
	from (
		select cat_id,t2.keyword_id,count(*) as num from (
			select a.cat_id,initcap(b.keyword) as keyword from tmp_cat a
			inner join tmp_cat_keyword b on a.goods_id=b.goods_id
			where a.cat_name!=b.keyword
			union all
			select a.cat_id,initcap(c.keyword) as keyword from tmp_cat a
			inner join tmp_value_cat c on a.goods_id=c.goods_id
			where a.cat_name!=c.cat_name
		) t1 inner join ads.ads_vova_keyword t2 on t1.keyword=t2.keyword_name group by cat_id,t2.keyword_id
);


with tmp_usable as (
	select goods_id,t1.attr_value,second_cat_id,third_cat_id,fourth_cat_id from ads.ads_vova_goods_attribute_merge t1
	inner join (select * from ads.ads_vova_usable_value) t2 on t1.attr_value = t2.attr_value
	where t1.pt = '${cur_date}'
),

tmp_brand as (
	select a.goods_id,b.brand_name as keyword from dim.dim_vova_goods a
	left join ods_vova_vts.ods_vova_brand b on a.brand_id=b.brand_id
	where a.brand_id>0 and a.is_on_sale=1
),

tmp_cat as (
	select distinct a.goods_id,keyword from (
	select goods_id,second_cat_name as keyword from dim.dim_vova_goods where second_cat_name is not null and is_on_sale=1
	union all
	select goods_id,third_cat_name as keyword from dim.dim_vova_goods where third_cat_name is not null and is_on_sale=1
	union all
	select goods_id,fourth_cat_name as keyword from dim.dim_vova_goods where fourth_cat_name is not null and is_on_sale=1
	) a
),

tmp_value_cat as (
	select distinct a.goods_id,keyword from (
		select goods_id,concat(attr_value,' ',b.second_cat_name) as keyword from tmp_usable a
		left join (select distinct second_cat_id,second_cat_name from dim.dim_vova_category) b on a.second_cat_id=b.second_cat_id
		where a.second_cat_id is not null
		union all
		select goods_id,concat(attr_value,' ',b.three_cat_name) as keyword from tmp_usable a
		left join (select distinct three_cat_id,three_cat_name from dim.dim_vova_category) b on a.third_cat_id=b.three_cat_id
		where a.third_cat_id is not null
		union all
		select goods_id,concat(attr_value,' ',b.four_cat_name) as keyword from tmp_usable a
		left join (select distinct four_cat_id,four_cat_name from dim.dim_vova_category) b on a.fourth_cat_id=b.four_cat_id
		where a.fourth_cat_id is not null
	) a
),

tmp_query as (
	select
      split(query_keys, '@@@')[0] query,
      Base64ToLongUDTF(goods_list) goods_id
    from
      mlb.mlb_vova_highfreq_query_match_d
    where pt ='${cur_date}'
)

insert into table ads.ads_vova_result_page_keyword_rank_data PARTITION (pt = '${cur_date}')
select 3 as type,query as type_value,keyword_id,row_number() over(partition by query order by num desc) as rank
from (
	select query,t2.keyword_id,count(*) as num from (
    select t1.query,initcap(t2.keyword) as keyword from tmp_query t1
    inner join tmp_brand t2 on t1.goods_id=t2.goods_id
    where t1.query!=t2.keyword
    union all
    select t1.query,initcap(t3.keyword) as keyword from tmp_query t1
	inner join tmp_cat t3 on t1.goods_id=t3.goods_id
	where t1.query!=t3.keyword
	union all
	select t1.query,initcap(t4.keyword) as keyword from tmp_query t1
	inner join tmp_value_cat t4 on t1.goods_id=t4.goods_id
	where t1.query!=t4.keyword
	) t inner join ads.ads_vova_keyword t2 on t.keyword=t2.keyword_name group by query,t2.keyword_id
);"

spark-sql --conf "spark.app.name=ads_vova_list_and_search_keyword_rank_data" --conf "spark.dynamicAllocation.maxExecutors=300" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi


