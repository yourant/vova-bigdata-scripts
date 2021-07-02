INSERT OVERWRITE table dwd.dwd_fd_order_channel_analytics
SELECT 
ogi.order_id, 
ogi.order_sn,
ogi.user_id,
ogi.user_agent_id, 
if(ogi.is_app is null, 'other', if(ogi.is_app = 0, 'web', 'mob')) AS platform,
ogi.platform_type, 
ogi.device_type, 
ogi.order_time, 
ogi.pay_status,
ogi.pay_time, 
ogi.country as country_id, 
ogi.country_code, 
ogi.language_id, 
ogi.language_code,
ogi.order_currency_id, 
ogi.order_currency_code, 
ogi.party_id,
ogi.project_name, 
ogi.goods_id, 
ogi.goods_name, 
ogi.goods_sn,
ogi.goods_sku,
ogi.cat_id, 
ogi.cat_name, 
ogi.goods_number, 
ogi.market_price, 
ogi.shop_price,
ogi.bonus, 
ogi.version as app_version, 
ogi.virtual_goods_id, 
ogi.integral, 
ogi.email, 
ogi.coupon_code,
ooa.oa_id, 
ooa.source, 
ooa.keyword, 
ooa.landing_page,
ooa.country, 
ooa.region, 
ooa.city,
ooa.campaign,
ooa.adformat,
ooa.adgroup,
ooa.adwordscampaignid, 
ooa.adwordsadgroupid,
ooa.devicecategory,
ooa.order_sn as order_sn_an,
ooa.origin_source, 
ooa.origin_medium, 
ooa.ga_channel, 
ooa.last_update_time
FROM dwd.dwd_fd_order_goods ogi
LEFT JOIN ods_fd_ar.ods_fd_order_analytics ooa ON ooa.order_id = ogi.order_id;
