set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_order_info_arc PARTITION (pt = '${pt}')
select 
     order_id, party_id, order_sn, user_id, order_time, order_status, shipping_status, pay_status, consignee, sex, country, province, city, district, address, zipcode, tel, mobile, email, best_time, sign_building, postscript, shipping_id, shipping_name, pay_id, pay_name, how_oos, how_surplus, pack_name, card_name, card_message, inv_payee, inv_content, inv_address, inv_zipcode, inv_phone, goods_amount, shipping_fee, duty_fee, insure_fee, shipping_proxy_fee, pay_fee, pack_fee, card_fee, money_paid, surplus, integral, integral_money, bonus, order_amount, from_ad, referer, confirm_time, pay_time, shipping_time, reserved_time, pack_id, card_id, bonus_id, invoice_no, extension_code, extension_id, to_buyer, pay_note, invoice_status, carrier_bill_id, receiving_time, biaoju_store_id, parent_order_id, track_id, real_paid, real_shipping_fee, is_shipping_fee_clear, is_order_amount_clear, is_ship_emailed, proxy_amount, pay_method, is_back, is_finance_clear, finance_clear_type, handle_time, start_shipping_time, end_shipping_time, shortage_status, is_shortage_await, order_type_id, is_display, special_type_id, misc_fee, additional_amount, distributor_id, taobao_order_sn, distribution_purchase_order_sn, need_invoice, facility_id, currency, language_id, track_status, defray_time
from (

    select 
        pt,order_id, party_id, order_sn, user_id, order_time, order_status, shipping_status, pay_status, consignee, sex, country, province, city, district, address, zipcode, tel, mobile, email, best_time, sign_building, postscript, shipping_id, shipping_name, pay_id, pay_name, how_oos, how_surplus, pack_name, card_name, card_message, inv_payee, inv_content, inv_address, inv_zipcode, inv_phone, goods_amount, shipping_fee, duty_fee, insure_fee, shipping_proxy_fee, pay_fee, pack_fee, card_fee, money_paid, surplus, integral, integral_money, bonus, order_amount, from_ad, referer, confirm_time, pay_time, shipping_time, reserved_time, pack_id, card_id, bonus_id, invoice_no, extension_code, extension_id, to_buyer, pay_note, invoice_status, carrier_bill_id, receiving_time, biaoju_store_id, parent_order_id, track_id, real_paid, real_shipping_fee, is_shipping_fee_clear, is_order_amount_clear, is_ship_emailed, proxy_amount, pay_method, is_back, is_finance_clear, finance_clear_type, handle_time, start_shipping_time, end_shipping_time, shortage_status, is_shortage_await, order_type_id, is_display, special_type_id, misc_fee, additional_amount, distributor_id, taobao_order_sn, distribution_purchase_order_sn, need_invoice, facility_id, currency, language_id, track_status, defray_time,
        row_number () OVER (PARTITION BY order_id ORDER BY pt DESC) AS rank
    from (

        select  pt,
                order_id,
                party_id,
                order_sn,
                user_id,
                order_time,
                order_status,
                shipping_status,
                pay_status,
                consignee,
                sex,
                country,
                province,
                city,
                district,
                address,
                zipcode,
                tel,
                mobile,
                email,
                best_time,
                sign_building,
                postscript,
                shipping_id,
                shipping_name,
                pay_id,
                pay_name,
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
                shipping_fee,
                duty_fee,
                insure_fee,
                shipping_proxy_fee,
                pay_fee,
                pack_fee,
                card_fee,
                money_paid,
                surplus,
                integral,
                integral_money,
                bonus,
                order_amount,
                from_ad,
                referer,
                confirm_time,
                pay_time,
                shipping_time,
                reserved_time,
                pack_id,
                card_id,
                bonus_id,
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
                is_display,
                special_type_id,
                misc_fee,
                additional_amount,
                distributor_id,
                taobao_order_sn,
                distribution_purchase_order_sn,
                need_invoice,
                facility_id,
                currency,
                language_id,
                track_status,
                defray_time
        from ods_fd_ecshop.ods_fd_ecs_order_info_arc where pt  = '${pt_last}'

        UNION

        select  pt,
                order_id,
                party_id,
                order_sn,
                user_id,
                order_time,
                order_status,
                shipping_status,
                pay_status,
                consignee,
                sex,
                country,
                province,
                city,
                district,
                address,
                zipcode,
                tel,
                mobile,
                email,
                best_time,
                sign_building,
                postscript,
                shipping_id,
                shipping_name,
                pay_id,
                pay_name,
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
                shipping_fee,
                duty_fee,
                insure_fee,
                shipping_proxy_fee,
                pay_fee,
                pack_fee,
                card_fee,
                money_paid,
                surplus,
                integral,
                integral_money,
                bonus,
                order_amount,
                from_ad,
                referer,
                confirm_time,
                pay_time,
                shipping_time,
                reserved_time,
                pack_id,
                card_id,
                bonus_id,
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
                is_display,
                special_type_id,
                misc_fee,
                additional_amount,
                distributor_id,
                taobao_order_sn,
                distribution_purchase_order_sn,
                need_invoice,
                facility_id,
                currency,
                language_id,
                track_status,
                defray_time
        from ods_fd_ecshop.ods_fd_ecs_order_info_inc where pt='${pt}'
    ) arc 
) tab where tab.rank = 1;
