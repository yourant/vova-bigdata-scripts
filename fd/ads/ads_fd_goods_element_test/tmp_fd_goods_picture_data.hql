SET hive.exec.compress.output=true;

insert overwrite table tmp.tmp_fd_goods_picture_test
select
/*+ REPARTITION(20) */
goods_id
,project
,case when platform='web' then 'PC' when platform='mob' then 'app' when platform='h5' then 'H5' else 'other' end as platform
,country
,rtype
,list_type
,picture_group
,picture_batch
,uv
FROM dwb.dwb_fd_goods_picture_uv
WHERE pt >= '${pt_begin}' and pt <= '${pt_end}'
and list_type in ("list-category")
and ((picture_group is not null and picture_group != '') or (picture_batch is not null and picture_batch!=''))
and goods_id is not null  and goods_id !=''
and length(country) < 3
;

