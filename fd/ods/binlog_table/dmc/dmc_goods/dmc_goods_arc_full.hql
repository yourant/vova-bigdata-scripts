CREATE TABLE IF NOT EXISTS ods_fd_dmc.ods_fd_dmc_goods_arc (
    id bigint comment 'id',
	goods_id bigint comment '网站id',
	created_at bigint comment '生成时间戳bigint',
	updated_at bigint comment '更新时间戳bigint',
	cat_id bigint comment '品类',
	fashion_buyer string comment 'fashionbuyer',
	provider_code string comment '公司代码',
	provider string comment '供应商',
	small_cat string comment '小分类',
	style string comment '风格',
	most_origin_goods_id string comment '最原始款id',
	most_origin_virtual_goods_id string comment '最原始款虚拟id',
	origin_goods_id string comment '对应原款id',
	origin_virtual_goods_id string comment '对应原款虚拟id',
	site_origin_goods_id string comment '同网站原款id',
	site_origin_virtual_goods_id string comment '同网站原款虚拟id',
	under_price string comment '低于市场定价',
	pricing_note string comment '商品定价备注',
	stylewe_price string comment 'stylewe售价（美金）',
	purchase_user string comment '采购提供人',
	provider_goods_sn string comment '贸综编码',
	goods_sn string comment '产品编号',
	competing_goods_review string comment '竞对review',
	note_one string comment '备注1',
	purchase_price string comment '采购价(rmb)',
	periods_sn string comment '运营期数',
	recept_develop_commodity string comment '二次开发款',
	channel string comment 'channel',
	channel_url string comment '渠道链接',
	goods_url string comment '商品link',
	provider_index_url string comment '供应商首页链接',
	provider_purchase_url string comment '下单链接',
	standby_note_one string comment '备用链接1',
	standby_note_two string comment '备用链接2',
	site_id bigint comment '竞品网站ID'
) COMMENT 'dmc商品表'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_dmc.ods_fd_dmc_goods_arc PARTITION (dt = '${hiveconf:dt}')
select 
     id, goods_id, created_at, updated_at, cat_id, fashion_buyer, provider_code, provider, small_cat, style, most_origin_goods_id, most_origin_virtual_goods_id, origin_goods_id, origin_virtual_goods_id, site_origin_goods_id, site_origin_virtual_goods_id, under_price, pricing_note, stylewe_price, purchase_user, provider_goods_sn, goods_sn, competing_goods_review, note_one, purchase_price, periods_sn, recept_develop_commodity, channel, channel_url, goods_url, provider_index_url, provider_purchase_url, standby_note_one, standby_note_two, site_id
from (

    select 
        dt,id, goods_id, created_at, updated_at, cat_id, fashion_buyer, provider_code, provider, small_cat, style, most_origin_goods_id, most_origin_virtual_goods_id, origin_goods_id, origin_virtual_goods_id, site_origin_goods_id, site_origin_virtual_goods_id, under_price, pricing_note, stylewe_price, purchase_user, provider_goods_sn, goods_sn, competing_goods_review, note_one, purchase_price, periods_sn, recept_develop_commodity, channel, channel_url, goods_url, provider_index_url, provider_purchase_url, standby_note_one, standby_note_two, site_id,
        row_number () OVER (PARTITION BY id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt,
                id,
                goods_id,
                if(created_at != '0000-00-00 00:00:00', unix_timestamp(created_at, "yyyy-MM-dd HH:mm:ss"), 0) AS created_at,
                if(updated_at != '0000-00-00 00:00:00', unix_timestamp(updated_at, "yyyy-MM-dd HH:mm:ss"), 0) AS updated_at,
                cat_id,
                fashion_buyer,
                provider_code,
                provider,
                small_cat,
                style,
                most_origin_goods_id,
                most_origin_virtual_goods_id,
                origin_goods_id,
                origin_virtual_goods_id,
                site_origin_goods_id,
                site_origin_virtual_goods_id,
                under_price,
                pricing_note,
                stylewe_price,
                purchase_user,
                provider_goods_sn,
                goods_sn,
                competing_goods_review,
                note_one,
                purchase_price,
                periods_sn,
                recept_develop_commodity,
                channel,
                channel_url,
                goods_url,
                provider_index_url,
                provider_purchase_url,
                standby_note_one,
                standby_note_two,
                site_id
        from tmp.tmp_fd_dmc_goods_full

        UNION

        select dt,id, goods_id, created_at, updated_at, cat_id, fashion_buyer, provider_code, provider, small_cat, style, most_origin_goods_id, most_origin_virtual_goods_id, origin_goods_id, origin_virtual_goods_id, site_origin_goods_id, site_origin_virtual_goods_id, under_price, pricing_note, stylewe_price, purchase_user, provider_goods_sn, goods_sn, competing_goods_review, note_one, purchase_price, periods_sn, recept_develop_commodity, channel, channel_url, goods_url, provider_index_url, provider_purchase_url, standby_note_one, standby_note_two, site_id
        from (

            select  dt,
                    id,
                    goods_id,
                    created_at,
                    updated_at,
                    cat_id,
                    fashion_buyer,
                    provider_code,
                    provider,
                    small_cat,
                    style,
                    most_origin_goods_id,
                    most_origin_virtual_goods_id,
                    origin_goods_id,
                    origin_virtual_goods_id,
                    site_origin_goods_id,
                    site_origin_virtual_goods_id,
                    under_price,
                    pricing_note,
                    stylewe_price,
                    purchase_user,
                    provider_goods_sn,
                    goods_sn,
                    competing_goods_review,
                    note_one,
                    purchase_price,
                    periods_sn,
                    recept_develop_commodity,
                    channel,
                    channel_url,
                    goods_url,
                    provider_index_url,
                    provider_purchase_url,
                    standby_note_one,
                    standby_note_two,
                    site_id,
                    row_number() OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_dmc.ods_fd_dmc_goods_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
