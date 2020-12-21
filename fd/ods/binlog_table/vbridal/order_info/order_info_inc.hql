INSERT overwrite table ods_fd_vb.ods_fd_order_info_inc PARTITION (pt= '${hiveconf:pt}')
select order_id ,party_id ,order_sn ,user_id ,order_time ,order_status ,shipping_status ,pay_status ,consignee ,gender ,country ,province ,province_text ,city ,city_text ,district ,district_text ,address ,zipcode ,tel ,mobile ,email ,best_time ,sign_building ,postscript ,important_day ,sm_id ,shipping_id ,shipping_name ,payment_id ,payment_name ,how_oos ,how_surplus ,pack_name ,card_name ,card_message ,inv_payee ,inv_content ,inv_address ,inv_zipcode ,inv_phone ,goods_amount ,goods_amount_exchange ,shipping_fee ,duty_fee ,shipping_fee_exchange ,duty_fee_exchange ,insure_fee ,shipping_proxy_fee ,payment_fee ,pack_fee ,card_fee ,money_paid ,surplus ,integral ,integral_money ,bonus ,bonus_exchange ,order_amount ,base_currency_id ,order_currency_id ,order_currency_symbol ,rate ,order_amount_exchange ,from_ad ,referer ,confirm_time ,pay_time ,shipping_time ,shipping_date_estimate ,shipping_carrier ,shipping_tracking_number ,pack_id ,card_id ,coupon_code ,invoice_no ,extension_code ,extension_id ,to_buyer ,pay_note ,invoice_status ,carrier_bill_id ,receiving_time ,biaoju_store_id ,parent_order_id ,track_id ,ga_track_id ,real_paid ,real_shipping_fee ,is_shipping_fee_clear ,is_order_amount_clear ,is_ship_emailed ,proxy_amount ,pay_method ,is_back ,is_finance_clear ,finance_clear_type ,handle_time ,start_shipping_time ,end_shipping_time ,shortage_status ,is_shortage_await ,order_type_id ,special_type_id ,is_display ,misc_fee ,additional_amount ,distributor_id ,taobao_order_sn ,distribution_purchase_order_sn ,need_invoice ,facility_id ,language_id ,coupon_cat_id ,coupon_config_value ,coupon_config_coupon_type ,is_conversion ,from_domain ,project_name ,user_agent_id ,display_currency_id ,display_currency_rate ,display_shipping_fee_exchange ,display_duty_fee_exchange ,display_order_amount_exchange ,display_goods_amount_exchange ,display_bonus_exchange ,token ,payer_id
from(
    select
        o_raw.xid AS event_id,
        o_raw.`table` AS event_table,
        o_raw.type AS event_type,
        cast(o_raw.`commit` AS BOOLEAN) AS event_commit,
        cast(o_raw.ts AS BIGINT) AS event_date,
        cast(o_raw.order_id AS INT) AS order_id,
        cast(o_raw.party_id AS INT) AS party_id,
        o_raw.order_sn AS order_sn,
        cast(o_raw.user_id AS INT) AS user_id,
        o_raw.order_time as order_time_original,
        o_raw.order_time AS order_time,
        cast(o_raw.order_status AS INT) AS order_status,
        cast(o_raw.shipping_status AS INT) AS shipping_status,
        cast(o_raw.pay_status AS INT) AS pay_status,
        o_raw.consignee AS consignee,
        o_raw.gender AS gender,
        o_raw.country AS country,
        o_raw.province AS province,
        o_raw.province_text AS province_text,
        cast(o_raw.city AS SMALLINT) AS city,
        o_raw.city_text AS city_text,
        cast(o_raw.district AS SMALLINT) AS district,
        o_raw.district_text AS district_text,
        o_raw.address AS address,
        o_raw.zipcode AS zipcode,
        o_raw.tel AS tel,
        o_raw.mobile AS mobile,
        o_raw.email AS email,
        o_raw.best_time AS best_time,
        o_raw.sign_building AS sign_building,
        o_raw.postscript AS postscript,
        cast(o_raw.important_day as date) AS important_day,
        cast(o_raw.sm_id AS SMALLINT) AS sm_id,
        cast(o_raw.shipping_id AS TINYINT) AS shipping_id,
        o_raw.shipping_name AS shipping_name,
        cast(o_raw.payment_id AS SMALLINT) AS payment_id,
        o_raw.payment_name AS payment_name,
        o_raw.how_oos AS how_oos,
        o_raw.how_surplus AS how_surplus,
        o_raw.pack_name AS pack_name,
        o_raw.card_name AS card_name,
        o_raw.card_message AS card_message,
        o_raw.inv_payee AS inv_payee,
        o_raw.inv_content AS inv_content,
        o_raw.inv_address AS inv_address,
        o_raw.inv_zipcode AS inv_zipcode,
        o_raw.inv_phone AS inv_phone,
        cast(o_raw.goods_amount AS DECIMAL(10, 2)) AS goods_amount,
        cast(o_raw.goods_amount_exchange AS DECIMAL(10, 2)) AS goods_amount_exchange,
        cast(o_raw.shipping_fee AS DECIMAL(10, 2)) AS shipping_fee,
        cast(o_raw.duty_fee AS DECIMAL(10, 2)) AS duty_fee,
        cast(o_raw.shipping_fee_exchange AS DECIMAL(10, 2)) AS shipping_fee_exchange,
        cast(o_raw.duty_fee_exchange AS DECIMAL(10, 2)) AS duty_fee_exchange,
        cast(o_raw.insure_fee AS DECIMAL(10, 2)) AS insure_fee,
        cast(o_raw.shipping_proxy_fee AS DECIMAL(10, 2)) AS shipping_proxy_fee,
        cast(o_raw.payment_fee AS DECIMAL(10, 2)) AS payment_fee,
        cast(o_raw.pack_fee AS DECIMAL(10, 2)) AS pack_fee,
        cast(o_raw.card_fee AS DECIMAL(10, 2)) AS card_fee,
        cast(o_raw.money_paid AS DECIMAL(10, 2)) AS money_paid,
        cast(o_raw.surplus AS DECIMAL(10, 2)) AS surplus,
        cast(o_raw.integral AS INT) AS integral,
        cast(o_raw.integral_money AS DECIMAL(10, 2)) AS integral_money,
        cast(o_raw.bonus AS DECIMAL(10, 2)) AS bonus,
        cast(o_raw.bonus_exchange AS DECIMAL(10, 2)) AS bonus_exchange,
        cast(o_raw.order_amount AS DECIMAL(10, 2)) AS order_amount,
        cast(o_raw.base_currency_id AS SMALLINT) AS base_currency_id,
        cast(o_raw.order_currency_id AS SMALLINT) AS order_currency_id,
        o_raw.order_currency_symbol AS order_currency_symbol,
        o_raw.rate AS rate,
        cast(o_raw.order_amount_exchange AS DECIMAL(10, 2)) AS order_amount_exchange,
        cast(o_raw.from_ad AS SMALLINT) AS from_ad,
        o_raw.referer AS referer,
        cast(o_raw.confirm_time AS BIGINT) AS confirm_time,
        o_raw.pay_time as pay_time_original,
        o_raw.pay_time AS pay_time,
        cast(o_raw.shipping_time AS BIGINT) AS shipping_time,
        cast(o_raw.shipping_date_estimate as date) AS shipping_date_estimate,
        o_raw.shipping_carrier AS shipping_carrier,
        o_raw.shipping_tracking_number AS shipping_tracking_number,
        cast(o_raw.pack_id AS TINYINT) AS pack_id,
        cast(o_raw.card_id AS TINYINT) AS card_id,
        o_raw.coupon_code AS coupon_code,
        o_raw.invoice_no AS invoice_no,
        o_raw.extension_code AS extension_code,
        cast(o_raw.extension_id AS INT) AS extension_id,
        o_raw.to_buyer AS to_buyer,
        o_raw.pay_note AS pay_note,
        cast(o_raw.invoice_status AS TINYINT) AS invoice_status,
        cast(o_raw.carrier_bill_id AS TINYINT) AS carrier_bill_id,
        cast(o_raw.receiving_time AS BIGINT) AS receiving_time,
        cast(o_raw.biaoju_store_id AS INT) AS biaoju_store_id,
        cast(o_raw.parent_order_id AS INT) AS parent_order_id,
        o_raw.track_id AS track_id,
        o_raw.ga_track_id AS ga_track_id,
        cast(o_raw.real_paid AS DECIMAL(10, 2)) AS real_paid,
        cast(o_raw.real_shipping_fee AS DECIMAL(10, 2)) AS real_shipping_fee,
        cast(o_raw.is_shipping_fee_clear AS TINYINT) AS is_shipping_fee_clear,
        cast(o_raw.is_order_amount_clear AS TINYINT) AS is_order_amount_clear,
        cast(o_raw.is_ship_emailed AS TINYINT) AS is_ship_emailed,
        cast(o_raw.proxy_amount AS DECIMAL(10, 2)) AS proxy_amount,
        o_raw.pay_method AS pay_method,
        o_raw.is_back AS is_back,
        cast(o_raw.is_finance_clear AS TINYINT) AS is_finance_clear,
        cast(o_raw.finance_clear_type AS TINYINT) AS finance_clear_type,
        cast(o_raw.handle_time AS BIGINT) AS handle_time,
        cast(o_raw.start_shipping_time AS BIGINT) AS start_shipping_time,
        cast(o_raw.end_shipping_time AS BIGINT) AS end_shipping_time,
        cast(o_raw.shortage_status AS TINYINT) AS shortage_status,
        o_raw.is_shortage_await AS is_shortage_await,
        o_raw.order_type_id AS order_type_id,
        o_raw.special_type_id AS special_type_id,
        cast(o_raw.is_display AS CHAR(1)) AS is_display,
        cast(o_raw.misc_fee AS DECIMAL(10, 2)) AS misc_fee,
        cast(o_raw.additional_amount AS DECIMAL(10, 2)) AS additional_amount,
        cast(o_raw.distributor_id AS INT) AS distributor_id,
        o_raw.taobao_order_sn AS taobao_order_sn,
        o_raw.distribution_purchase_order_sn AS distribution_purchase_order_sn,
        cast(o_raw.need_invoice AS CHAR(1)) AS need_invoice,
        o_raw.facility_id AS facility_id,
        cast(o_raw.language_id AS INT) AS language_id,
        cast(o_raw.coupon_cat_id AS SMALLINT) AS coupon_cat_id,
        cast(o_raw.coupon_config_value AS DECIMAL(10, 2)) AS coupon_config_value,
        o_raw.coupon_config_coupon_type AS coupon_config_coupon_type,
        cast(o_raw.is_conversion AS TINYINT) AS is_conversion,
        o_raw.from_domain AS from_domain,
        o_raw.project_name AS project_name,
        cast(o_raw.user_agent_id AS INT) AS user_agent_id,
        cast(o_raw.display_currency_id AS INT) AS display_currency_id,
        o_raw.display_currency_rate AS display_currency_rate,
        cast(o_raw.display_shipping_fee_exchange AS DECIMAL(10, 2)) AS display_shipping_fee_exchange,
        cast(o_raw.display_duty_fee_exchange AS DECIMAL(10, 2)) AS display_duty_fee_exchange,
        cast(o_raw.display_order_amount_exchange AS DECIMAL(10, 2)) AS display_order_amount_exchange,
        cast(o_raw.display_goods_amount_exchange AS DECIMAL(10, 2)) AS display_goods_amount_exchange,
        cast(o_raw.display_bonus_exchange AS DECIMAL(10, 2)) AS display_bonus_exchange,
        o_raw.token AS token,
        o_raw.payer_id AS payer_id,
        row_number () OVER (PARTITION BY o_raw.order_id ORDER BY cast(o_raw.xid as BIGINT) DESC) AS rank
    from pdb.fd_vb_order_info
    LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type', 'kafka_old','order_id', 'party_id', 'order_sn', 'user_id', 'order_time', 'order_status', 'shipping_status', 'pay_status', 'consignee', 'gender', 'country', 'province', 'province_text', 'city', 'city_text', 'district', 'district_text', 'address', 'zipcode', 'tel', 'mobile', 'email', 'best_time', 'sign_building', 'postscript', 'important_day', 'sm_id', 'shipping_id', 'shipping_name', 'payment_id', 'payment_name', 'how_oos', 'how_surplus', 'pack_name', 'card_name', 'card_message', 'inv_payee', 'inv_content', 'inv_address', 'inv_zipcode', 'inv_phone', 'goods_amount', 'goods_amount_exchange', 'shipping_fee', 'duty_fee', 'shipping_fee_exchange', 'duty_fee_exchange', 'insure_fee', 'shipping_proxy_fee', 'payment_fee', 'pack_fee', 'card_fee', 'money_paid', 'surplus', 'integral', 'integral_money', 'bonus', 'bonus_exchange', 'order_amount', 'base_currency_id', 'order_currency_id', 'order_currency_symbol', 'rate', 'order_amount_exchange', 'from_ad', 'referer', 'confirm_time', 'pay_time', 'shipping_time', 'shipping_date_estimate', 'shipping_carrier', 'shipping_tracking_number', 'pack_id', 'card_id', 'coupon_code', 'invoice_no', 'extension_code', 'extension_id', 'to_buyer', 'pay_note', 'invoice_status', 'carrier_bill_id', 'receiving_time', 'biaoju_store_id', 'parent_order_id', 'track_id', 'ga_track_id', 'real_paid', 'real_shipping_fee', 'is_shipping_fee_clear', 'is_order_amount_clear', 'is_ship_emailed', 'proxy_amount', 'pay_method', 'is_back', 'is_finance_clear', 'finance_clear_type', 'handle_time', 'start_shipping_time', 'end_shipping_time', 'shortage_status', 'is_shortage_await', 'order_type_id', 'special_type_id', 'is_display', 'misc_fee', 'additional_amount', 'distributor_id', 'taobao_order_sn', 'distribution_purchase_order_sn', 'need_invoice', 'facility_id', 'language_id', 'coupon_cat_id', 'coupon_config_value', 'coupon_config_coupon_type', 'is_conversion', 'from_domain', 'project_name', 'user_agent_id', 'display_currency_id', 'display_currency_rate', 'display_shipping_fee_exchange', 'display_duty_fee_exchange', 'display_order_amount_exchange', 'display_goods_amount_exchange', 'display_bonus_exchange', 'token', 'payer_id') o_raw
    AS `table`, ts, `commit`, xid, type, old, order_id ,party_id ,order_sn ,user_id ,order_time ,order_status ,shipping_status ,pay_status ,consignee ,gender ,country ,province ,province_text ,city ,city_text ,district ,district_text ,address ,zipcode ,tel ,mobile ,email ,best_time ,sign_building ,postscript ,important_day ,sm_id ,shipping_id ,shipping_name ,payment_id ,payment_name ,how_oos ,how_surplus ,pack_name ,card_name ,card_message ,inv_payee ,inv_content ,inv_address ,inv_zipcode ,inv_phone ,goods_amount ,goods_amount_exchange ,shipping_fee ,duty_fee ,shipping_fee_exchange ,duty_fee_exchange ,insure_fee ,shipping_proxy_fee ,payment_fee ,pack_fee ,card_fee ,money_paid ,surplus ,integral ,integral_money ,bonus ,bonus_exchange ,order_amount ,base_currency_id ,order_currency_id ,order_currency_symbol ,rate ,order_amount_exchange ,from_ad ,referer ,confirm_time ,pay_time ,shipping_time ,shipping_date_estimate ,shipping_carrier ,shipping_tracking_number ,pack_id ,card_id ,coupon_code ,invoice_no ,extension_code ,extension_id ,to_buyer ,pay_note ,invoice_status ,carrier_bill_id ,receiving_time ,biaoju_store_id ,parent_order_id ,track_id ,ga_track_id ,real_paid ,real_shipping_fee ,is_shipping_fee_clear ,is_order_amount_clear ,is_ship_emailed ,proxy_amount ,pay_method ,is_back ,is_finance_clear ,finance_clear_type ,handle_time ,start_shipping_time ,end_shipping_time ,shortage_status ,is_shortage_await ,order_type_id ,special_type_id ,is_display ,misc_fee ,additional_amount ,distributor_id ,taobao_order_sn ,distribution_purchase_order_sn ,need_invoice ,facility_id ,language_id ,coupon_cat_id ,coupon_config_value ,coupon_config_coupon_type ,is_conversion ,from_domain ,project_name ,user_agent_id ,display_currency_id ,display_currency_rate ,display_shipping_fee_exchange ,display_duty_fee_exchange ,display_order_amount_exchange ,display_goods_amount_exchange ,display_bonus_exchange ,token ,payer_id
    where pt = '${hiveconf:pt}'
) inc where inc.rank = 1;
