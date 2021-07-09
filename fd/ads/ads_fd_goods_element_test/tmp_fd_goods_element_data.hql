SET hive.exec.compress.output=true;

insert overwrite table tmp.tmp_fd_goods_element_test
select
/*+ REPARTITION(20) */
,goods_id
,project
,case when platform='web' then 'PC' when platform='mob' then 'app' when platform='h5' then 'H5' else 'other' end as platform
,country
,rtype
,page_code
,element_name
,element_tag
,element_batch
,uv
FROM dwb.dwb_fd_goods_element_uv
WHERE  page_code = 'product'
and pt >= '${pt_begin}' and pt <= '${pt_end}'
and ((element_name is not null and element_name != '') or (element_tag is not null and element_tag != ''))
and trim(element_name) in ('add_to_cart', 'benefits_add_to_cart', 'direct_add_to_cart', 'goods_detail_add', 'play_video')
and goods_id is not null and goods_id !=''
and length(country) < 3
;

