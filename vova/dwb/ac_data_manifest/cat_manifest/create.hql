-- �����ݡ�[4947]ACƷ��������ݱ���
-- ɸѡ��
-- ʱ��
-- ����	Ĭ��չʾall	չʾtop20����	gb,fr,de,it,es,nl,pt,es,us,cs,pl,be,mx,si,ru,jp,br,tw,na,au
-- Ʒ��	Ĭ��չʾall	all��������Ʒ��
--
--
-- search���ݱ���
-- 		��չʾ������ɸѡ		"searchҳ����
-- ��ͬ����Ʒ���Ӧͬһһ��Ʒ��������ͬ"	searchҳ����	����Ʒ����������Ʒ�ӹ���ťUV/����ҳUV	����Ʒ����������Ʒ�Ѹ���UV/����Ʒ������ҳUV
-- ʱ��	����	һ��Ʒ��	����Ʒ��	һ��Ʒ����uv	����Ʒ����UV	����ҳת����	���굽֧��ת����

AC���ݱ���-search���ݱ���

Drop table dwb.dwb_vova_second_cat_manifest;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_second_cat_manifest (
datasource           string    COMMENT 'd_����Դ',
region_code          string    COMMENT 'd_����',
first_cat_name       string    COMMENT 'i_һ��Ʒ��',
second_cat_name      string    COMMENT 'd_����Ʒ��',

search_first_cat_uv  bigint    COMMENT 'i_searchҳ����һ��Ʒ����UV',
search_second_cat_uv bigint    COMMENT 'i_searchҳ���ݶ���Ʒ����UV',
add_cart_uv          bigint    COMMENT 'i_����Ʒ����������Ʒ�ӹ���ťUV',
pd_uv                bigint    COMMENT 'i_����ҳUV',
pay_uv               bigint    COMMENT 'i_��Ʒ֧��UV'
) COMMENT 'AC���ݱ���-search���ݱ���' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_second_cat_manifest/"
;

select *
from dwb.dwb_vova_second_cat_manifest
where region_code='all' and second_cat_name ='all'
