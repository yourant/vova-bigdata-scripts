select
log.page_code,
log.pt,
count(*) as impression_pv
from
dwd.dwd_vova_log_goods_impression log
WHERE log.datasource = 'vova'
AND log.pt = ''
AND log.platform = 'mob'
group by log.page_code, log.pt
impression_pv <= 5000
;

select
log.page_code,
log.pt,
count(*) as impression_pv
from
dwd.dwd_vova_log_goods_impression log
WHERE log.datasource = 'vova'
AND log.pt = ''
AND log.platform = 'mob'
group by log.page_code, log.pt
impression_pv <= 5000
;


