CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_order_info_arc (
    order_id BIGINT,
    event_date BIGINT,
    party_id BIGINT,
    order_sn STRING,
    user_id BIGINT,
    order_time_original STRING,
    order_time BIGINT,
    order_status BIGINT,
    shipping_status BIGINT,
    pay_status BIGINT,
    consignee STRING,
    gender STRING,
    country BIGINT,
    province BIGINT,
    province_text STRING COMMENT '省/直辖市，输入',
    city BIGINT,
    city_text STRING COMMENT '市/区，输入',
    district BIGINT,
    district_text STRING COMMENT '县/区，输入',
    address STRING,
    zipcode STRING,
    tel STRING,
    mobile STRING,
    email STRING,
    best_time STRING,
    sign_building STRING,
    postscript STRING COMMENT '订单附言',
    important_day BIGINT COMMENT 'The Date of Your Important Day (Wedding, Prom, Party, etc)',
    sm_id BIGINT COMMENT 'shipping method id',
    shipping_id BIGINT,
    shipping_name STRING,
    payment_id BIGINT,
    payment_name STRING,
    how_oos STRING,
    how_surplus STRING,
    pack_name STRING,
    card_name STRING,
    card_message STRING,
    inv_payee STRING,
    inv_content STRING,
    inv_address STRING COMMENT '发票寄送地址',
    inv_zipcode STRING COMMENT '发票寄送地址邮编',
    inv_phone STRING COMMENT '发票到的电话',
    goods_amount DECIMAL(15, 4),
    goods_amount_exchange DECIMAL(15, 4) COMMENT '商品转换后的数额',
    shipping_fee DECIMAL(15, 4),
    duty_fee DECIMAL(15, 4) COMMENT '税金金额',
    shipping_fee_exchange DECIMAL(15, 4),
    duty_fee_exchange DECIMAL(15, 4) COMMENT '税金交易金额',
    insure_fee DECIMAL(15, 4),
    shipping_proxy_fee DECIMAL(15, 4),
    payment_fee DECIMAL(15, 4),
    pack_fee DECIMAL(15, 4),
    card_fee DECIMAL(15, 4),
    money_paid DECIMAL(15, 4),
    surplus DECIMAL(15, 4),
    integral BIGINT COMMENT '已经抵用欧币',
    integral_money DECIMAL(15, 4),
    bonus DECIMAL(15, 4) COMMENT '优惠费用，负值',
    bonus_exchange DECIMAL(15, 4),
    order_amount DECIMAL(15, 4),
    base_currency_id BIGINT COMMENT '币种ID',
    order_currency_id BIGINT COMMENT '生成订单时用户选择的币种',
    order_currency_symbol STRING COMMENT 'like US$ HK$',
    rate STRING COMMENT '字符串：exchange/base',
    order_amount_exchange DECIMAL(15, 4) COMMENT '转换后的数额',
    from_ad BIGINT,
    referer STRING,
    confirm_time BIGINT,
    pay_time_original STRING,
    pay_time BIGINT,
    shipping_time BIGINT,
    shipping_date_estimate STRING COMMENT '预计发货日期',
    shipping_carrier STRING,
    shipping_tracking_number STRING COMMENT '货运单号',
    pack_id BIGINT,
    card_id BIGINT,
    coupon_code STRING COMMENT '优惠券代码',
    invoice_no STRING,
    extension_code STRING,
    extension_id BIGINT,
    to_buyer STRING,
    pay_note STRING,
    invoice_status BIGINT,
    carrier_bill_id BIGINT,
    receiving_time BIGINT,
    biaoju_store_id BIGINT,
    parent_order_id BIGINT,
    track_id STRING,
    ga_track_id STRING COMMENT 'ga跟踪ID',
    real_paid DECIMAL(15, 4),
    real_shipping_fee DECIMAL(15, 4),
    is_shipping_fee_clear BIGINT,
    is_order_amount_clear BIGINT,
    is_ship_emailed BIGINT,
    proxy_amount DECIMAL(15, 4) COMMENT '代收货款费用',
    pay_method STRING,
    is_back STRING COMMENT '是否从快递公司追回已发送货物',
    is_finance_clear BIGINT COMMENT '财务是否清算',
    finance_clear_type BIGINT COMMENT '财务清算类型',
    handle_time BIGINT COMMENT '处理时间，该时间点前的订单不显示在待配货页面',
    start_shipping_time BIGINT COMMENT '起始快递时间，小时',
    end_shipping_time BIGINT COMMENT '结束快递时间，小时',
    shortage_status BIGINT COMMENT '缺货状态：0有货；1暂缺货；2请等待；3已到货；4取消',
    is_shortage_await STRING COMMENT '缺货等待',
    order_type_id STRING,
    special_type_id STRING COMMENT '特殊类型定义',
    is_display STRING COMMENT '是否显示给客服',
    misc_fee DECIMAL(15, 4) COMMENT '????',
    additional_amount DECIMAL(15, 4) COMMENT '-h订单还需要增加的费用',
    distributor_id BIGINT COMMENT '分销商',
    taobao_order_sn STRING,
    distribution_purchase_order_sn STRING COMMENT '分销采购单号',
    need_invoice STRING COMMENT '是否需要发票',
    facility_id STRING COMMENT '仓库id',
    language_id BIGINT,
    coupon_cat_id BIGINT,
    coupon_config_value DECIMAL(15, 4) COMMENT '@see ok_coupon_config',
    coupon_config_coupon_type STRING COMMENT '@see ok_coupon_config',
    is_conversion BIGINT COMMENT '数据是否已提交给google adwords',
    from_domain STRING COMMENT '订单来源',
    project_name STRING,
    user_agent_id BIGINT COMMENT '下单时的 user agent',
    display_currency_id BIGINT COMMENT '下单时显示的货币',
    display_currency_rate STRING COMMENT '下单时显示的货币汇率',
    display_shipping_fee_exchange DECIMAL(15, 4) COMMENT '显示用运费',
    display_duty_fee_exchange DECIMAL(15, 4) COMMENT '税金显示金额',
    display_order_amount_exchange DECIMAL(15, 4) COMMENT '显示用订单总金额',
    display_goods_amount_exchange DECIMAL(15, 4) COMMENT '显示用商品总金额',
    display_bonus_exchange DECIMAL(15, 4) COMMENT '显示用coupon费',
    token STRING COMMENT 'paypal express checkout token标记',
    payer_id STRING COMMENT 'paypal payer id'
) COMMENT '来自kafka订单每日增量数据'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_vb.ods_fd_order_info_arc PARTITION (dt = '${hiveconf:dt}')
select 
      order_id,event_date, party_id, order_sn, user_id, order_time_original, order_time, order_status, shipping_status, pay_status, consignee, gender, country, province, province_text, city, city_text, district, district_text, address, zipcode, tel, mobile, email, best_time, sign_building, postscript, important_day, sm_id, shipping_id, shipping_name, payment_id, payment_name, how_oos, how_surplus, pack_name, card_name, card_message, inv_payee, inv_content, inv_address, inv_zipcode, inv_phone, goods_amount, goods_amount_exchange, shipping_fee, duty_fee, shipping_fee_exchange, duty_fee_exchange, insure_fee, shipping_proxy_fee, payment_fee, pack_fee, card_fee, money_paid, surplus, integral, integral_money, bonus, bonus_exchange, order_amount, base_currency_id, order_currency_id, order_currency_symbol, rate, order_amount_exchange, from_ad, referer, confirm_time, pay_time_original,pay_time, shipping_time, shipping_date_estimate, shipping_carrier, shipping_tracking_number, pack_id, card_id, coupon_code, invoice_no, extension_code, extension_id, to_buyer, pay_note, invoice_status, carrier_bill_id, receiving_time, biaoju_store_id, parent_order_id, track_id, ga_track_id, real_paid, real_shipping_fee, is_shipping_fee_clear, is_order_amount_clear, is_ship_emailed, proxy_amount, pay_method, is_back, is_finance_clear, finance_clear_type, handle_time, start_shipping_time, end_shipping_time, shortage_status, is_shortage_await, order_type_id, special_type_id, is_display, misc_fee, additional_amount, distributor_id, taobao_order_sn, distribution_purchase_order_sn, need_invoice, facility_id, language_id, coupon_cat_id, coupon_config_value, coupon_config_coupon_type, is_conversion, from_domain, project_name, user_agent_id, display_currency_id, display_currency_rate, display_shipping_fee_exchange, display_duty_fee_exchange, display_order_amount_exchange, display_goods_amount_exchange, display_bonus_exchange, token, payer_id
from (

    select 
        dt,order_id, event_date, party_id, order_sn, user_id, order_time_original, order_time, order_status, shipping_status, pay_status, consignee, gender, country, province, province_text, city, city_text, district, district_text, address, zipcode, tel, mobile, email, best_time, sign_building, postscript, important_day, sm_id, shipping_id, shipping_name, payment_id, payment_name, how_oos, how_surplus, pack_name, card_name, card_message, inv_payee, inv_content, inv_address, inv_zipcode, inv_phone, goods_amount, goods_amount_exchange, shipping_fee, duty_fee, shipping_fee_exchange, duty_fee_exchange, insure_fee, shipping_proxy_fee, payment_fee, pack_fee, card_fee, money_paid, surplus, integral, integral_money, bonus, bonus_exchange, order_amount, base_currency_id, order_currency_id, order_currency_symbol, rate, order_amount_exchange, from_ad, referer, confirm_time, pay_time_original,pay_time, shipping_time, shipping_date_estimate, shipping_carrier, shipping_tracking_number, pack_id, card_id, coupon_code, invoice_no, extension_code, extension_id, to_buyer, pay_note, invoice_status, carrier_bill_id, receiving_time, biaoju_store_id, parent_order_id, track_id, ga_track_id, real_paid, real_shipping_fee, is_shipping_fee_clear, is_order_amount_clear, is_ship_emailed, proxy_amount, pay_method, is_back, is_finance_clear, finance_clear_type, handle_time, start_shipping_time, end_shipping_time, shortage_status, is_shortage_await, order_type_id, special_type_id, is_display, misc_fee, additional_amount, distributor_id, taobao_order_sn, distribution_purchase_order_sn, need_invoice, facility_id, language_id, coupon_cat_id, coupon_config_value, coupon_config_coupon_type, is_conversion, from_domain, project_name, user_agent_id, display_currency_id, display_currency_rate, display_shipping_fee_exchange, display_duty_fee_exchange, display_order_amount_exchange, display_goods_amount_exchange, display_bonus_exchange, token, payer_id,
        row_number () OVER (PARTITION BY order_id ORDER BY dt DESC) AS rank
    from (

        select  dt
                order_id,
		event_date,
                party_id,
                order_sn,
                user_id,
                order_time_original,
                order_time,
                order_status,
                shipping_status,
                pay_status,
                consignee,
                gender,
                country,
                province,
                province_text,
                city,
                city_text,
                district,
                district_text,
                address,
                zipcode,
                tel,
                mobile,
                email,
                best_time,
                sign_building,
                postscript,
                important_day,
                sm_id,
                shipping_id,
                shipping_name,
                payment_id,
                payment_name,
                how_oos,
                how_surplus,
                pack_name,
                card_name,
                card_message,
                inv_payee,
                inv_content,
                inv_address,
                inv_zipcode,
                inv_phone,
                goods_amount,
                goods_amount_exchange,
                shipping_fee,
                duty_fee,
                shipping_fee_exchange,
                duty_fee_exchange,
                insure_fee,
                shipping_proxy_fee,
                payment_fee,
                pack_fee,
                card_fee,
                money_paid,
                surplus,
                integral,
                integral_money,
                bonus,
                bonus_exchange,
                order_amount,
                base_currency_id,
                order_currency_id,
                order_currency_symbol,
                rate,
                order_amount_exchange,
                from_ad,
                referer,
                confirm_time,
                pay_time_original,
                pay_time,
                shipping_time,
                shipping_date_estimate,
                shipping_carrier,
                shipping_tracking_number,
                pack_id,
                card_id,
                coupon_code,
                invoice_no,
                extension_code,
                extension_id,
                to_buyer,
                pay_note,
                invoice_status,
                carrier_bill_id,
                receiving_time,
                biaoju_store_id,
                parent_order_id,
                track_id,
                ga_track_id,
                real_paid,
                real_shipping_fee,
                is_shipping_fee_clear,
                is_order_amount_clear,
                is_ship_emailed,
                proxy_amount,
                pay_method,
                is_back,
                is_finance_clear,
                finance_clear_type,
                handle_time,
                start_shipping_time,
                end_shipping_time,
                shortage_status,
                is_shortage_await,
                order_type_id,
                special_type_id,
                is_display,
                misc_fee,
                additional_amount,
                distributor_id,
                taobao_order_sn,
                distribution_purchase_order_sn,
                need_invoice,
                facility_id,
                language_id,
                coupon_cat_id,
                coupon_config_value,
                coupon_config_coupon_type,
                is_conversion,
                from_domain,
                project_name,
                user_agent_id,
                display_currency_id,
                display_currency_rate,
                display_shipping_fee_exchange,
                display_duty_fee_exchange,
                display_order_amount_exchange,
                display_goods_amount_exchange,
                display_bonus_exchange,
                token,
                payer_id
        from ods_fd_vb.ods_fd_order_info_arc where dt  = '${hiveconf:dt_last}'
        UNION
        select dt,order_id,event_date, party_id, order_sn, user_id, order_time,order_time_original, order_status, shipping_status, pay_status, consignee, gender, country, province, province_text, city, city_text, district, district_text, address, zipcode, tel, mobile, email, best_time, sign_building, postscript, important_day, sm_id, shipping_id, shipping_name, payment_id, payment_name, how_oos, how_surplus, pack_name, card_name, card_message, inv_payee, inv_content, inv_address, inv_zipcode, inv_phone, goods_amount, goods_amount_exchange, shipping_fee, duty_fee, shipping_fee_exchange, duty_fee_exchange, insure_fee, shipping_proxy_fee, payment_fee, pack_fee, card_fee, money_paid, surplus, integral, integral_money, bonus, bonus_exchange, order_amount, base_currency_id, order_currency_id, order_currency_symbol, rate, order_amount_exchange, from_ad, referer, confirm_time, pay_time_original,pay_time, shipping_time, shipping_date_estimate, shipping_carrier, shipping_tracking_number, pack_id, card_id, coupon_code, invoice_no, extension_code, extension_id, to_buyer, pay_note, invoice_status, carrier_bill_id, receiving_time, biaoju_store_id, parent_order_id, track_id, ga_track_id, real_paid, real_shipping_fee, is_shipping_fee_clear, is_order_amount_clear, is_ship_emailed, proxy_amount, pay_method, is_back, is_finance_clear, finance_clear_type, handle_time, start_shipping_time, end_shipping_time, shortage_status, is_shortage_await, order_type_id, special_type_id, is_display, misc_fee, additional_amount, distributor_id, taobao_order_sn, distribution_purchase_order_sn, need_invoice, facility_id, language_id, coupon_cat_id, coupon_config_value, coupon_config_coupon_type, is_conversion, from_domain, project_name, user_agent_id, display_currency_id, display_currency_rate, display_shipping_fee_exchange, display_duty_fee_exchange, display_order_amount_exchange, display_goods_amount_exchange, display_bonus_exchange, token, payer_id
        from (

            select  '${hiveconf:dt}' as dt,
                    order_id,
		    event_date,
                    party_id,
                    order_sn,
                    user_id,
                    order_time_original,
                    order_time,
                    order_status,
                    shipping_status,
                    pay_status,
                    consignee,
                    gender,
                    country,
                    province,
                    province_text,
                    city,
                    city_text,
                    district,
                    district_text,
                    address,
                    zipcode,
                    tel,
                    mobile,
                    email,
                    best_time,
                    sign_building,
                    postscript,
                    important_day,
                    sm_id,
                    shipping_id,
                    shipping_name,
                    payment_id,
                    payment_name,
                    how_oos,
                    how_surplus,
                    pack_name,
                    card_name,
                    card_message,
                    inv_payee,
                    inv_content,
                    inv_address,
                    inv_zipcode,
                    inv_phone,
                    goods_amount,
                    goods_amount_exchange,
                    shipping_fee,
                    duty_fee,
                    shipping_fee_exchange,
                    duty_fee_exchange,
                    insure_fee,
                    shipping_proxy_fee,
                    payment_fee,
                    pack_fee,
                    card_fee,
                    money_paid,
                    surplus,
                    integral,
                    integral_money,
                    bonus,
                    bonus_exchange,
                    order_amount,
                    base_currency_id,
                    order_currency_id,
                    order_currency_symbol,
                    rate,
                    order_amount_exchange,
                    from_ad,
                    referer,
                    confirm_time,
                    pay_time_original,
                    pay_time,
                    shipping_time,
                    shipping_date_estimate,
                    shipping_carrier,
                    shipping_tracking_number,
                    pack_id,
                    card_id,
                    coupon_code,
                    invoice_no,
                    extension_code,
                    extension_id,
                    to_buyer,
                    pay_note,
                    invoice_status,
                    carrier_bill_id,
                    receiving_time,
                    biaoju_store_id,
                    parent_order_id,
                    track_id,
                    ga_track_id,
                    real_paid,
                    real_shipping_fee,
                    is_shipping_fee_clear,
                    is_order_amount_clear,
                    is_ship_emailed,
                    proxy_amount,
                    pay_method,
                    is_back,
                    is_finance_clear,
                    finance_clear_type,
                    handle_time,
                    start_shipping_time,
                    end_shipping_time,
                    shortage_status,
                    is_shortage_await,
                    order_type_id,
                    special_type_id,
                    is_display,
                    misc_fee,
                    additional_amount,
                    distributor_id,
                    taobao_order_sn,
                    distribution_purchase_order_sn,
                    need_invoice,
                    facility_id,
                    language_id,
                    coupon_cat_id,
                    coupon_config_value,
                    coupon_config_coupon_type,
                    is_conversion,
                    from_domain,
                    project_name,
                    user_agent_id,
                    display_currency_id,
                    display_currency_rate,
                    display_shipping_fee_exchange,
                    display_duty_fee_exchange,
                    display_order_amount_exchange,
                    display_goods_amount_exchange,
                    display_bonus_exchange,
                    token,
                    payer_id,
                    row_number () OVER (PARTITION BY order_id ORDER BY event_id DESC) AS rank
            from ods_fd_vb.ods_fd_order_info_inc where dt = '${hiveconf:dt}'
        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
