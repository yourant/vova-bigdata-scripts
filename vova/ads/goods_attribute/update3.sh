#!/bin/bash
reg='\\&|\\"|\\/|\\^|#|\\\n|\\\t|\\\r|\\|,|,|，|`|\\;|!|\\[|\\]|\\+|\\*|\\?|:|。|《|》|\\<|\\>|_|\\{|\\}\\~|\\@|\\¥|=|、|%|\\$'
sql="
insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,7 as cat_attr_id,a.second_cat_id from (
	select goods_id,'styles' as attr_key,styles as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where styles is not null
 union all
select goods_id,'decoration' as attr_key,decoration as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where decoration is not null
 union all
select goods_id,'pattern_type' as attr_key,pattern_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where pattern_type is not null
 union all
select goods_id,'lifting_part_type' as attr_key,lifting_part_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where lifting_part_type is not null
 union all
select goods_id,'dandbag_type' as attr_key,dandbag_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where dandbag_type is not null
 union all
select goods_id,'applicable_gender' as attr_key,applicable_gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where applicable_gender is not null
 union all
select goods_id,'suitable_age' as attr_key,suitable_age as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where suitable_age is not null
 union all
select goods_id,'applications' as attr_key,applications as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where applications is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where style is not null
 union all
select goods_id,'lining_material' as attr_key,lining_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where lining_material is not null
 union all
select goods_id,'types' as attr_key,types as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where types is not null
 union all
select goods_id,'capacity' as attr_key,capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where capacity is not null
 union all
select goods_id,'internal_structure' as attr_key,internal_structure as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where internal_structure is not null
 union all
select goods_id,'opening_type' as attr_key,opening_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where opening_type is not null
 union all
select goods_id,'number_of_shoulder_straps' as attr_key,number_of_shoulder_straps as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where number_of_shoulder_straps is not null
 union all
select goods_id,'craft' as attr_key,craft as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where craft is not null
 union all
select goods_id,'size' as attr_key,size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where size is not null
 union all
select goods_id,'product_type' as attr_key,product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where product_type is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where material is not null
 union all
select goods_id,'backpack_type' as attr_key,backpack_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where backpack_type is not null
 union all
select goods_id,'waterproof_cover' as attr_key,waterproof_cover as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where waterproof_cover is not null
 union all
select goods_id,'carrying_system' as attr_key,carrying_system as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where carrying_system is not null
 union all
select goods_id,'customproperty' as attr_key,customproperty as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where customproperty is not null
 union all
select goods_id,'type_of_outer_bag' as attr_key,type_of_outer_bag as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where type_of_outer_bag is not null
 union all
select goods_id,'brand' as attr_key,brand as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where brand is not null
 union all
select goods_id,'model' as attr_key,model as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where model is not null
 union all
select goods_id,'luggage_type' as attr_key,luggage_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where luggage_type is not null
 union all
select goods_id,'suitcase_size' as attr_key,suitcase_size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where suitcase_size is not null
 union all
select goods_id,'luggage_accessories' as attr_key,luggage_accessories as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where luggage_accessories is not null
 union all
select goods_id,'applicable_musical_instrument' as attr_key,applicable_musical_instrument as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where applicable_musical_instrument is not null
 union all
select goods_id,'with_or_without_tie_rod' as attr_key,with_or_without_tie_rod as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where with_or_without_tie_rod is not null
 union all
select goods_id,'with_lock' as attr_key,with_lock as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where with_lock is not null
 union all
select goods_id,'casters' as attr_key,casters as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where casters is not null
 union all
select goods_id,'applicable_cards' as attr_key,applicable_cards as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where applicable_cards is not null
 union all
select goods_id,'wallet_type' as attr_key,wallet_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_bag where wallet_type is not null
) as a;

insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,8 as cat_attr_id,a.second_cat_id from (
	select goods_id,'three_glasses_type' as attr_key,three_glasses_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where three_glasses_type is not null
 union all
select goods_id,'audio_format' as attr_key,audio_format as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where audio_format is not null
 union all
select goods_id,'application' as attr_key,application as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where application is not null
 union all
select goods_id,'application_people' as attr_key,application_people as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where application_people is not null
 union all
select goods_id,'audio_divider' as attr_key,audio_divider as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where audio_divider is not null
 union all
select goods_id,'applicable_activity' as attr_key,applicable_activity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where applicable_activity is not null
 union all
select goods_id,'application_age_group' as attr_key,application_age_group as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where application_age_group is not null
 union all
select goods_id,'anti_shake_type' as attr_key,anti_shake_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where anti_shake_type is not null
 union all
select goods_id,'battery_type' as attr_key,battery_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where battery_type is not null
 union all
select goods_id,'brightness' as attr_key,brightness as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where brightness is not null
 union all
select goods_id,'battery_capacity' as attr_key,battery_capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where battery_capacity is not null
 union all
select goods_id,'built_in_speaker' as attr_key,built_in_speaker as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where built_in_speaker is not null
 union all
select goods_id,'certification' as attr_key,certification as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where certification is not null
 union all
select goods_id,'contrast_ratio' as attr_key,contrast_ratio as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where contrast_ratio is not null
 union all
select goods_id,'cpu' as attr_key,cpu as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where cpu is not null
 union all
select goods_id,'connection' as attr_key,connection as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where connection is not null
 union all
select goods_id,'contrast' as attr_key,contrast as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where contrast is not null
 union all
select goods_id,'cpu_core_number' as attr_key,cpu_core_number as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where cpu_core_number is not null
 union all
select goods_id,'compatible_brand' as attr_key,compatible_brand as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where compatible_brand is not null
 union all
select goods_id,'cpu_graphics_card' as attr_key,cpu_graphics_card as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where cpu_graphics_card is not null
 union all
select goods_id,'cooling_type' as attr_key,cooling_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where cooling_type is not null
 union all
select goods_id,'call_model' as attr_key,call_model as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where call_model is not null
 union all
select goods_id,'compatible_device' as attr_key,compatible_device as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where compatible_device is not null
 union all
select goods_id,'capacity' as attr_key,capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where capacity is not null
 union all
select goods_id,'channels' as attr_key,channels as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where channels is not null
 union all
select goods_id,'camera_type' as attr_key,camera_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where camera_type is not null
 union all
select goods_id,'compatible_action_camera_brand' as attr_key,compatible_action_camera_brand as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where compatible_action_camera_brand is not null
 union all
select goods_id,'charging_type' as attr_key,charging_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where charging_type is not null
 union all
select goods_id,'decoding_ability' as attr_key,decoding_ability as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where decoding_ability is not null
 union all
select goods_id,'display_size' as attr_key,display_size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where display_size is not null
 union all
select goods_id,'disc_format' as attr_key,disc_format as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where disc_format is not null
 union all
select goods_id,'external_memory' as attr_key,external_memory as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where external_memory is not null
 union all
select goods_id,'ethernet' as attr_key,ethernet as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where ethernet is not null
 union all
select goods_id,'effective_megapixel' as attr_key,effective_megapixel as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where effective_megapixel is not null
 union all
select goods_id,'features' as attr_key,features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where features is not null
 union all
select goods_id,'graphics_card_type' as attr_key,graphics_card_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where graphics_card_type is not null
 union all
select goods_id,'graphics_interface' as attr_key,graphics_interface as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where graphics_interface is not null
 union all
select goods_id,'hard_disk_speed' as attr_key,hard_disk_speed as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where hard_disk_speed is not null
 union all
select goods_id,'hard_disk_interface' as attr_key,hard_disk_interface as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where hard_disk_interface is not null
 union all
select goods_id,'hard_drive_capacity' as attr_key,hard_drive_capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where hard_drive_capacity is not null
 union all
select goods_id,'hd_support' as attr_key,hd_support as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where hd_support is not null
 union all
select goods_id,'imaging_ratio' as attr_key,imaging_ratio as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where imaging_ratio is not null
 union all
select goods_id,'electronics_interface' as attr_key,electronics_interface as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where electronics_interface is not null
 union all
select goods_id,'lens_uv_lens_size' as attr_key,lens_uv_lens_size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where lens_uv_lens_size is not null
 union all
select goods_id,'lens_type' as attr_key,lens_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where lens_type is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where material is not null
 union all
select goods_id,'maximum_expansion_capacity' as attr_key,maximum_expansion_capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where maximum_expansion_capacity is not null
 union all
select goods_id,'memory_capacity' as attr_key,memory_capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where memory_capacity is not null
 union all
select goods_id,'memory_type' as attr_key,memory_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where memory_type is not null
 union all
select goods_id,'megapixel' as attr_key,megapixel as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where megapixel is not null
 union all
select goods_id,'monitor_area' as attr_key,monitor_area as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where monitor_area is not null
 union all
select goods_id,'number_of_displays' as attr_key,number_of_displays as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where number_of_displays is not null
 union all
select goods_id,'operation_mode' as attr_key,operation_mode as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where operation_mode is not null
 union all
select goods_id,'origin' as attr_key,origin as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where origin is not null
 union all
select goods_id,'operating_system' as attr_key,operating_system as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where operating_system is not null
 union all
select goods_id,'output_power' as attr_key,output_power as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where output_power is not null
 union all
select goods_id,'optical_zoom' as attr_key,optical_zoom as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where optical_zoom is not null
 union all
select goods_id,'power' as attr_key,power as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where power is not null
 union all
select goods_id,'packing_list' as attr_key,packing_list as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where packing_list is not null
 union all
select goods_id,'plug_type' as attr_key,plug_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where plug_type is not null
 union all
select goods_id,'product_type' as attr_key,product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where product_type is not null
 union all
select goods_id,'power_supply' as attr_key,power_supply as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where power_supply is not null
 union all
select goods_id,'plug_interface' as attr_key,plug_interface as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where plug_interface is not null
 union all
select goods_id,'pixel' as attr_key,pixel as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where pixel is not null
 union all
select goods_id,'pixels' as attr_key,pixels as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where pixels is not null
 union all
select goods_id,'placement' as attr_key,placement as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where placement is not null
 union all
select goods_id,'resolution' as attr_key,resolution as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where resolution is not null
 union all
select goods_id,'support_bluetooth' as attr_key,support_bluetooth as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where support_bluetooth is not null
 union all
select goods_id,'screen_type' as attr_key,screen_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where screen_type is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where style is not null
 union all
select goods_id,'storage' as attr_key,storage as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where storage is not null
 union all
select goods_id,'size' as attr_key,size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where size is not null
 union all
select goods_id,'screen_shape' as attr_key,screen_shape as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where screen_shape is not null
 union all
select goods_id,'strap_material' as attr_key,strap_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where strap_material is not null
 union all
select goods_id,'strap_color' as attr_key,strap_color as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where strap_color is not null
 union all
select goods_id,'tv_type' as attr_key,tv_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where tv_type is not null
 union all
select goods_id,'type' as attr_key,type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where type is not null
 union all
select goods_id,'transmission_rate' as attr_key,transmission_rate as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where transmission_rate is not null
 union all
select goods_id,'use' as attr_key,use as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where use is not null
 union all
select goods_id,'usb_ports' as attr_key,usb_ports as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where usb_ports is not null
 union all
select goods_id,'video_output' as attr_key,video_output as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where video_output is not null
 union all
select goods_id,'video_format' as attr_key,video_format as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where video_format is not null
 union all
select goods_id,'voltage' as attr_key,voltage as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where voltage is not null
 union all
select goods_id,'wristband_material' as attr_key,wristband_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where wristband_material is not null
 union all
select goods_id,'rom' as attr_key,rom as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where rom is not null
 union all
select goods_id,'ram' as attr_key,ram as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_electronics where ram is not null
) as a;

insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,9 as cat_attr_id,a.second_cat_id from (
	select goods_id,'gender' as attr_key,gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where gender is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where material is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where style is not null
 union all
select goods_id,'season' as attr_key,season as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where season is not null
 union all
select goods_id,'colour' as attr_key,colour as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where colour is not null
 union all
select goods_id,'sleeve_length' as attr_key,sleeve_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where sleeve_length is not null
 union all
select goods_id,'pattern_type' as attr_key,pattern_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where pattern_type is not null
 union all
select goods_id,'collar' as attr_key,collar as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where collar is not null
 union all
select goods_id,'product_type' as attr_key,product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where product_type is not null
 union all
select goods_id,'types_of' as attr_key,types_of as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where types_of is not null
 union all
select goods_id,'occasion' as attr_key,occasion as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where occasion is not null
 union all
select goods_id,'decoration' as attr_key,decoration as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where decoration is not null
 union all
select goods_id,'fabric_type' as attr_key,fabric_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where fabric_type is not null
 union all
select goods_id,'size' as attr_key,size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where size is not null
 union all
select goods_id,'sleeve_type' as attr_key,sleeve_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where sleeve_type is not null
 union all
select goods_id,'waist_type' as attr_key,waist_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where waist_type is not null
 union all
select goods_id,'top_length' as attr_key,top_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where top_length is not null
 union all
select goods_id,'product_features' as attr_key,product_features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where product_features is not null
 union all
select goods_id,'thickness' as attr_key,thickness as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where thickness is not null
 union all
select goods_id,'skirt_length' as attr_key,skirt_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where skirt_length is not null
 union all
select goods_id,'stuffing' as attr_key,stuffing as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where stuffing is not null
 union all
select goods_id,'audience_age' as attr_key,audience_age as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where audience_age is not null
 union all
select goods_id,'placket' as attr_key,placket as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where placket is not null
 union all
select goods_id,'board_type' as attr_key,board_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where board_type is not null
 union all
select goods_id,'shirt_type' as attr_key,shirt_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where shirt_type is not null
 union all
select goods_id,'pants_style' as attr_key,pants_style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where pants_style is not null
 union all
select goods_id,'features' as attr_key,features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where features is not null
 union all
select goods_id,'top_type' as attr_key,top_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where top_type is not null
 union all
select goods_id,'pants_length' as attr_key,pants_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where pants_length is not null
 union all
select goods_id,'coat_type' as attr_key,coat_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where coat_type is not null
 union all
select goods_id,'color_style' as attr_key,color_style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where color_style is not null
 union all
select goods_id,'origin' as attr_key,origin as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where origin is not null
 union all
select goods_id,'lining_material' as attr_key,lining_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where lining_material is not null
 union all
select goods_id,'washing_type' as attr_key,washing_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where washing_type is not null
 union all
select goods_id,'skirt_type' as attr_key,skirt_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_womencloth where skirt_type is not null
) as a;

insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,10 as cat_attr_id,a.second_cat_id from (
	select goods_id,'gender' as attr_key,gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where gender is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where material is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where style is not null
 union all
select goods_id,'season' as attr_key,season as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where season is not null
 union all
select goods_id,'colour' as attr_key,colour as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where colour is not null
 union all
select goods_id,'sleeve_length' as attr_key,sleeve_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where sleeve_length is not null
 union all
select goods_id,'pattern_type' as attr_key,pattern_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where pattern_type is not null
 union all
select goods_id,'collar' as attr_key,collar as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where collar is not null
 union all
select goods_id,'product_type' as attr_key,product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where product_type is not null
 union all
select goods_id,'types_of' as attr_key,types_of as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where types_of is not null
 union all
select goods_id,'occasion' as attr_key,occasion as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where occasion is not null
 union all
select goods_id,'decoration' as attr_key,decoration as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where decoration is not null
 union all
select goods_id,'fabric_type' as attr_key,fabric_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where fabric_type is not null
 union all
select goods_id,'size' as attr_key,size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where size is not null
 union all
select goods_id,'sleeve_type' as attr_key,sleeve_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where sleeve_type is not null
 union all
select goods_id,'waist_type' as attr_key,waist_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where waist_type is not null
 union all
select goods_id,'top_length' as attr_key,top_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where top_length is not null
 union all
select goods_id,'product_features' as attr_key,product_features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where product_features is not null
 union all
select goods_id,'thickness' as attr_key,thickness as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where thickness is not null
 union all
select goods_id,'stuffing' as attr_key,stuffing as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where stuffing is not null
 union all
select goods_id,'audience_age' as attr_key,audience_age as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where audience_age is not null
 union all
select goods_id,'placket' as attr_key,placket as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where placket is not null
 union all
select goods_id,'board_type' as attr_key,board_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where board_type is not null
 union all
select goods_id,'shirt_type' as attr_key,shirt_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where shirt_type is not null
 union all
select goods_id,'features' as attr_key,features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where features is not null
 union all
select goods_id,'top_type' as attr_key,top_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where top_type is not null
 union all
select goods_id,'pants_length' as attr_key,pants_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where pants_length is not null
 union all
select goods_id,'coat_type' as attr_key,coat_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where coat_type is not null
 union all
select goods_id,'color_style' as attr_key,color_style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where color_style is not null
 union all
select goods_id,'origin' as attr_key,origin as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where origin is not null
 union all
select goods_id,'lining_material' as attr_key,lining_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where lining_material is not null
 union all
select goods_id,'washing_type' as attr_key,washing_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_mancloth where washing_type is not null
) as a;

insert overwrite table ads.ads_vova_goods_attribute_label_data
select goods_id,attr_key,
concat_ws(' ', sentences(
lower(
trim(
regexp_replace(
regexp_replace(val, '$reg', ' ')
,'[\\\s]+',' ')
)
))[0]) as attr_value,
cat_attr_id,second_cat_id from ads.ads_vova_goods_pre_attribute_data
lateral view explode(attr_value) as val
"
spark-sql --conf "spark.app.name=ads_vova_goods_pre_attribute_data" --conf "spark.dynamicAllocation.maxExecutors=10" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi