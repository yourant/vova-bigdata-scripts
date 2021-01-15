insert overwrite table dim.dim_fd_currency
select currency_id,
       currency,
       currency_symbol,
       desc_en,
       desc_cn,
       disabled,
       display_order,
       currency_local_symbol,
       continent
from ods_fd_vb.ods_fd_currency;
