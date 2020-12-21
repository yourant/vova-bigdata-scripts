INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_goods_inc  PARTITION (pt='${hiveconf:pt}')
select goods_id, goods_party_id, cat_id, goods_sn, sku, goods_name, click_count, brand_id, provider_name, goods_number, goods_weight, goods_volume, market_price, shop_price, fitting_price, promote_price, promote_start, promote_end, warn_number, keywords, goods_brief, goods_desc, goods_thumb, goods_img, original_img, is_real, extension_code, is_on_sale, is_alone_sale, is_linked, is_basic, is_gift, can_handsel, integral, add_time, sort_order, is_delete, is_best, is_new, is_hot, is_promote, bonus_type_id, last_update, goods_type, seller_note, cycle_img, provider_id, goods_details, vote_times, vote_score, is_on_sale_pending, boost, limit_integral, top_cat_id, addtional_shipping_fee, vip_price, is_vip, is_remains, return_ratio, customized, is_same_price, sale_status, sale_status_detail, commonsense, is_shield, is_display, price_range, goods_name_short, identify, suit, clerk_comment, media_comment, os, resolution, java_support, bill, extra, barcode, uniq_sku, is_maintain_weight, external_cat_id, is_batch, product_id, external_goods_id, erp_sku, create_time
from(
    select
        o_raw.xid AS event_id
        ,o_raw.`table` AS event_table
        ,o_raw.type AS event_type
        ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
        ,cast(o_raw.ts AS BIGINT) AS event_date
        ,o_raw.goods_id
        ,o_raw.goods_party_id
        ,o_raw.cat_id
        ,o_raw.goods_sn
        ,o_raw.sku
        ,o_raw.goods_name
        ,o_raw.click_count
        ,o_raw.brand_id
        ,o_raw.provider_name
        ,o_raw.goods_number
        ,o_raw.goods_weight
        ,o_raw.goods_volume
        ,o_raw.market_price
        ,o_raw.shop_price
        ,o_raw.fitting_price
        ,o_raw.promote_price
        ,o_raw.promote_start
        ,o_raw.promote_end
        ,o_raw.warn_number
        ,o_raw.keywords
        ,o_raw.goods_brief
        ,o_raw.goods_desc
        ,o_raw.goods_thumb
        ,o_raw.goods_img
        ,o_raw.original_img
        ,o_raw.is_real
        ,o_raw.extension_code
        ,o_raw.is_on_sale
        ,o_raw.is_alone_sale
        ,o_raw.is_linked
        ,o_raw.is_basic
        ,o_raw.is_gift
        ,o_raw.can_handsel
        ,o_raw.integral
        ,o_raw.add_time
        ,o_raw.sort_order
        ,o_raw.is_delete
        ,o_raw.is_best
        ,o_raw.is_new
        ,o_raw.is_hot
        ,o_raw.is_promote
        ,o_raw.bonus_type_id
        ,o_raw.last_update
        ,o_raw.goods_type
        ,o_raw.seller_note
        ,o_raw.cycle_img
        ,o_raw.provider_id
        ,o_raw.goods_details
        ,o_raw.vote_times
        ,o_raw.vote_score
        ,o_raw.is_on_sale_pending
        ,o_raw.boost
        ,o_raw.limit_integral
        ,o_raw.top_cat_id
        ,o_raw.addtional_shipping_fee
        ,o_raw.vip_price
        ,o_raw.is_vip
        ,o_raw.is_remains
        ,o_raw.return_ratio
        ,o_raw.customized
        ,o_raw.is_same_price
        ,o_raw.sale_status
        ,o_raw.sale_status_detail
        ,o_raw.commonsense
        ,o_raw.is_shield
        ,o_raw.is_display
        ,o_raw.price_range
        ,o_raw.goods_name_short
        ,o_raw.identify
        ,o_raw.suit
        ,o_raw.clerk_comment
        ,o_raw.media_comment
        ,o_raw.os
        ,o_raw.resolution
        ,o_raw.java_support
        ,o_raw.bill
        ,o_raw.extra
        ,o_raw.barcode
        ,o_raw.uniq_sku
        ,o_raw.is_maintain_weight
        ,o_raw.external_cat_id
        ,o_raw.is_batch
        ,o_raw.product_id
        ,o_raw.external_goods_id
        ,o_raw.erp_sku
        ,o_raw.create_time
        ,row_number () OVER (PARTITION BY o_raw.goods_id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.fd_ecshop_ecs_goods
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'goods_id', 'goods_party_id', 'cat_id', 'goods_sn', 'sku', 'goods_name', 'click_count', 'brand_id', 'provider_name', 'goods_number', 'goods_weight', 'goods_volume', 'market_price', 'shop_price', 'fitting_price', 'promote_price', 'promote_start', 'promote_end', 'warn_number', 'keywords', 'goods_brief', 'goods_desc', 'goods_thumb', 'goods_img', 'original_img', 'is_real', 'extension_code', 'is_on_sale', 'is_alone_sale', 'is_linked', 'is_basic', 'is_gift', 'can_handsel', 'integral', 'add_time', 'sort_order', 'is_delete', 'is_best', 'is_new', 'is_hot', 'is_promote', 'bonus_type_id', 'last_update', 'goods_type', 'seller_note', 'cycle_img', 'provider_id', 'goods_details', 'vote_times', 'vote_score', 'is_on_sale_pending', 'boost', 'limit_integral', 'top_cat_id', 'addtional_shipping_fee', 'vip_price', 'is_vip', 'is_remains', 'return_ratio', 'customized', 'is_same_price', 'sale_status', 'sale_status_detail', 'commonsense', 'is_shield', 'is_display', 'price_range', 'goods_name_short', 'identify', 'suit', 'clerk_comment', 'media_comment', 'os', 'resolution', 'java_support', 'bill', 'extra', 'barcode', 'uniq_sku', 'is_maintain_weight', 'external_cat_id', 'is_batch', 'product_id', 'external_goods_id', 'erp_sku', 'create_time') o_raw
    AS `table`, ts, `commit`, xid, type, old, goods_id, goods_party_id, cat_id, goods_sn, sku, goods_name, click_count, brand_id, provider_name, goods_number, goods_weight, goods_volume, market_price, shop_price, fitting_price, promote_price, promote_start, promote_end, warn_number, keywords, goods_brief, goods_desc, goods_thumb, goods_img, original_img, is_real, extension_code, is_on_sale, is_alone_sale, is_linked, is_basic, is_gift, can_handsel, integral, add_time, sort_order, is_delete, is_best, is_new, is_hot, is_promote, bonus_type_id, last_update, goods_type, seller_note, cycle_img, provider_id, goods_details, vote_times, vote_score, is_on_sale_pending, boost, limit_integral, top_cat_id, addtional_shipping_fee, vip_price, is_vip, is_remains, return_ratio, customized, is_same_price, sale_status, sale_status_detail, commonsense, is_shield, is_display, price_range, goods_name_short, identify, suit, clerk_comment, media_comment, os, resolution, java_support, bill, extra, barcode, uniq_sku, is_maintain_weight, external_cat_id, is_batch, product_id, external_goods_id, erp_sku, create_time
    where pt='${hiveconf:pt}'
)inc where inc.rank = 1;
