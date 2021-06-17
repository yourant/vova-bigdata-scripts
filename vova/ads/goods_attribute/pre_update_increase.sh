#!/bin/bash
sql="
insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,11 as cat_attr_id,a.second_cat_id from (
	select goods_id,'product_height' as attr_key,product_height as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where product_height is not null
 union all
select goods_id,'product_type' as attr_key,product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where product_type is not null
 union all
select goods_id,'product_shape' as attr_key,product_shape as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where product_shape is not null
 union all
select goods_id,'product_weight' as attr_key,product_weight as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where product_weight is not null
 union all
select goods_id,'metal_type' as attr_key,metal_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where metal_type is not null
 union all
select goods_id,'brand' as attr_key,brand as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where brand is not null
 union all
select goods_id,'customproperty' as attr_key,customproperty as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where customproperty is not null
 union all
select goods_id,'is_it_adjustable' as attr_key,is_it_adjustable as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where is_it_adjustable is not null
 union all
select goods_id,'diy_jewelry_packaging_type' as attr_key,diy_jewelry_packaging_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where diy_jewelry_packaging_type is not null
 union all
select goods_id,'jewelry_tool_type' as attr_key,jewelry_tool_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where jewelry_tool_type is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where material is not null
 union all
select goods_id,'outer_diameter' as attr_key,outer_diameter as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where outer_diameter is not null
 union all
select goods_id,'kit_list' as attr_key,kit_list as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where kit_list is not null
 union all
select goods_id,'can_be_customized' as attr_key,can_be_customized as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where can_be_customized is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where style is not null
 union all
select goods_id,'occasion' as attr_key,occasion as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where occasion is not null
 union all
select goods_id,'product_width' as attr_key,product_width as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where product_width is not null
 union all
select goods_id,'product_features' as attr_key,product_features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where product_features is not null
 union all
select goods_id,'product_length' as attr_key,product_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where product_length is not null
 union all
select goods_id,'product_diameter' as attr_key,product_diameter as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where product_diameter is not null
 union all
select goods_id,'jewelry_accessories_type' as attr_key,jewelry_accessories_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where jewelry_accessories_type is not null
 union all
select goods_id,'metal_purity' as attr_key,metal_purity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where metal_purity is not null
 union all
select goods_id,'vice_stone' as attr_key,vice_stone as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where vice_stone is not null
 union all
select goods_id,'features' as attr_key,features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where features is not null
 union all
select goods_id,'jewelry_packaging_and_display_types' as attr_key,jewelry_packaging_and_display_types as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where jewelry_packaging_and_display_types is not null
 union all
select goods_id,'model' as attr_key,model as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where model is not null
 union all
select goods_id,'certificate_no' as attr_key,certificate_no as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where certificate_no is not null
 union all
select goods_id,'certificate_type' as attr_key,certificate_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where certificate_type is not null
 union all
select goods_id,'main_stone' as attr_key,main_stone as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where main_stone is not null
 union all
select goods_id,'colour' as attr_key,colour as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where colour is not null
 union all
select goods_id,'jewelry_or_accessories' as attr_key,jewelry_or_accessories as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where jewelry_or_accessories is not null
 union all
select goods_id,'shape_pattern' as attr_key,shape_pattern as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where shape_pattern is not null
 union all
select goods_id,'gender' as attr_key,gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where gender is not null
 union all
select goods_id,'weight' as attr_key,weight as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where weight is not null
 union all
select goods_id,'ornament_type' as attr_key,ornament_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where ornament_type is not null
 union all
select goods_id,'pendant_size' as attr_key,pendant_size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where pendant_size is not null
 union all
select goods_id,'compatibility' as attr_key,compatibility as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where compatibility is not null
 union all
select goods_id,'metal_color' as attr_key,metal_color as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where metal_color is not null
 union all
select goods_id,'pendant_type' as attr_key,pendant_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where pendant_type is not null
 union all
select goods_id,'chain_type' as attr_key,chain_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where chain_type is not null
 union all
select goods_id,'customized' as attr_key,customized as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where customized is not null
 union all
select goods_id,'necklace_type' as attr_key,necklace_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where necklace_type is not null
 union all
select goods_id,'size' as attr_key,size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where size is not null
 union all
select goods_id,'dimensions' as attr_key,dimensions as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where dimensions is not null
 union all
select goods_id,'earring_clasp' as attr_key,earring_clasp as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where earring_clasp is not null
 union all
select goods_id,'earring_type' as attr_key,earring_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where earring_type is not null
 union all
select goods_id,'plating' as attr_key,plating as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where plating is not null
 union all
select goods_id,'bracelet_type' as attr_key,bracelet_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where bracelet_type is not null
 union all
select goods_id,'buckle_type' as attr_key,buckle_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where buckle_type is not null
 union all
select goods_id,'diameter' as attr_key,diameter as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where diameter is not null
 union all
select goods_id,'mosaic_type' as attr_key,mosaic_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where mosaic_type is not null
 union all
select goods_id,'ring_type' as attr_key,ring_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where ring_type is not null
 union all
select goods_id,'ring_width' as attr_key,ring_width as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where ring_width is not null
 union all
select goods_id,'jewelry_set_type' as attr_key,jewelry_set_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where jewelry_set_type is not null
 union all
select goods_id,'type' as attr_key,type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where type is not null
 union all
select goods_id,'for_people' as attr_key,for_people as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where for_people is not null
 union all
select goods_id,'pattern_type' as attr_key,pattern_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where pattern_type is not null
 union all
select goods_id,'character' as attr_key,character as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where character is not null
 union all
select goods_id,'piercing_jewelry_body_jewelry_type' as attr_key,piercing_jewelry_body_jewelry_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where piercing_jewelry_body_jewelry_type is not null
 union all
select goods_id,'brooch_type' as attr_key,brooch_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where brooch_type is not null
 union all
select goods_id,'special_purpose' as attr_key,special_purpose as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where special_purpose is not null
 union all
select goods_id,'craft' as attr_key,craft as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where craft is not null
 union all
select goods_id,'glove_length' as attr_key,glove_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where glove_length is not null
 union all
select goods_id,'glove_size' as attr_key,glove_size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where glove_size is not null
 union all
select goods_id,'number_of_veil_layers' as attr_key,number_of_veil_layers as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where number_of_veil_layers is not null
 union all
select goods_id,'veil_side_style' as attr_key,veil_side_style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where veil_side_style is not null
 union all
select goods_id,'classification' as attr_key,classification as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where classification is not null
 union all
select goods_id,'category' as attr_key,category as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where category is not null
 union all
select goods_id,'bridal_gloves_style' as attr_key,bridal_gloves_style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where bridal_gloves_style is not null
 union all
select goods_id,'decoration' as attr_key,decoration as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where decoration is not null
 union all
select goods_id,'pearl_type' as attr_key,pearl_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where pearl_type is not null
 union all
select goods_id,'cufflinks_tie_clips' as attr_key,cufflinks_tie_clips as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where cufflinks_tie_clips is not null
 union all
select goods_id,'width' as attr_key,width as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where width is not null
 union all
select goods_id,'standard_voltage' as attr_key,standard_voltage as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where standard_voltage is not null
 union all
select goods_id,'specification' as attr_key,specification as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where specification is not null
 union all
select goods_id,'tool_type' as attr_key,tool_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where tool_type is not null
 union all
select goods_id,'certification' as attr_key,certification as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where certification is not null
 union all
select goods_id,'special_function' as attr_key,special_function as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where special_function is not null
 union all
select goods_id,'lens_width' as attr_key,lens_width as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where lens_width is not null
 union all
select goods_id,'lens_height' as attr_key,lens_height as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where lens_height is not null
 union all
select goods_id,'lens_material' as attr_key,lens_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where lens_material is not null
 union all
select goods_id,'frame_material' as attr_key,frame_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where frame_material is not null
 union all
select goods_id,'whether_with_automatic_cleaning_function' as attr_key,whether_with_automatic_cleaning_function as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where whether_with_automatic_cleaning_function is not null
 union all
select goods_id,'lens_color' as attr_key,lens_color as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where lens_color is not null
 union all
select goods_id,'lens_optical_properties' as attr_key,lens_optical_properties as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where lens_optical_properties is not null
 union all
select goods_id,'coating' as attr_key,coating as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where coating is not null
 union all
select goods_id,'length' as attr_key,length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where length is not null
 union all
select goods_id,'glasses_accessories' as attr_key,glasses_accessories as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where glasses_accessories is not null
 union all
select goods_id,'glasses_type' as attr_key,glasses_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where glasses_type is not null
 union all
select goods_id,'tie_type' as attr_key,tie_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where tie_type is not null
 union all
select goods_id,'scarf_length' as attr_key,scarf_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where scarf_length is not null
 union all
select goods_id,'scarf_style' as attr_key,scarf_style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where scarf_style is not null
 union all
select goods_id,'season' as attr_key,season as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where season is not null
 union all
select goods_id,'belt_buckle_width' as attr_key,belt_buckle_width as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where belt_buckle_width is not null
 union all
select goods_id,'belt_buckle_length' as attr_key,belt_buckle_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where belt_buckle_length is not null
 union all
select goods_id,'bandwidth' as attr_key,bandwidth as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where bandwidth is not null
 union all
select goods_id,'belt_material' as attr_key,belt_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where belt_material is not null
 union all
select goods_id,'certificate' as attr_key,certificate as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where certificate is not null
 union all
select goods_id,'pants_length' as attr_key,pants_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where pants_length is not null
 union all
select goods_id,'chain_length' as attr_key,chain_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where chain_length is not null
 union all
select goods_id,'safety_standard' as attr_key,safety_standard as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where safety_standard is not null
 union all
select goods_id,'protection_level' as attr_key,protection_level as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_accessories where protection_level is not null
	) a;
	insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,12 as cat_attr_id,a.second_cat_id from (
select goods_id,'accessories_product_type' as attr_key,accessories_product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where accessories_product_type is not null
 union all
select goods_id,'closure_type' as attr_key,closure_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where closure_type is not null
 union all
select goods_id,'colour' as attr_key,colour as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where colour is not null
 union all
select goods_id,'cycling_product_type' as attr_key,cycling_product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where cycling_product_type is not null
 union all
select goods_id,'fabric_type' as attr_key,fabric_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where fabric_type is not null
 union all
select goods_id,'fishing_product_type' as attr_key,fishing_product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where fishing_product_type is not null
 union all
select goods_id,'fitness_product_type' as attr_key,fitness_product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where fitness_product_type is not null
 union all
select goods_id,'gender' as attr_key,gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where gender is not null
 union all
select goods_id,'hiking_product_type' as attr_key,hiking_product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where hiking_product_type is not null
 union all
select goods_id,'hunting_product_type' as attr_key,hunting_product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where hunting_product_type is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where material is not null
 union all
select goods_id,'pattern_type' as attr_key,pattern_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where pattern_type is not null
 union all
select goods_id,'product_feature' as attr_key,product_feature as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where product_feature is not null
 union all
select goods_id,'sports_bags_type' as attr_key,sports_bags_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where sports_bags_type is not null
 union all
select goods_id,'sports_wear_type' as attr_key,sports_wear_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where sports_wear_type is not null
 union all
select goods_id,'team_sports_product_type' as attr_key,team_sports_product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where team_sports_product_type is not null
 union all
select goods_id,'water_sports_product_type' as attr_key,water_sports_product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_sports_outdoors where water_sports_product_type is not null
) a;
insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,13 as cat_attr_id,a.second_cat_id from (
select goods_id,'type' as attr_key,type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where type is not null
 union all
select goods_id,'quantity' as attr_key,quantity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where quantity is not null
 union all
select goods_id,'feature' as attr_key,feature as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where feature is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where material is not null
 union all
select goods_id,'size' as attr_key,size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where size is not null
 union all
select goods_id,'color' as attr_key,color as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where color is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where style is not null
 union all
select goods_id,'benefit' as attr_key,benefit as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where benefit is not null
 union all
select goods_id,'applicable_scene' as attr_key,applicable_scene as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where applicable_scene is not null
 union all
select goods_id,'gender' as attr_key,gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where gender is not null
 union all
select goods_id,'function' as attr_key,function as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where function is not null
 union all
select goods_id,'application_area' as attr_key,application_area as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where application_area is not null
 union all
select goods_id,'capacity' as attr_key,capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where capacity is not null
 union all
select goods_id,'weight' as attr_key,weight as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where weight is not null
 union all
select goods_id,'ingredient' as attr_key,ingredient as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where ingredient is not null
 union all
select goods_id,'usage_time' as attr_key,usage_time as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where usage_time is not null
 union all
select goods_id,'application_age_group' as attr_key,application_age_group as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where application_age_group is not null
 union all
select goods_id,'texture' as attr_key,texture as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where texture is not null
 union all
select goods_id,'suitable_for_skin_type' as attr_key,suitable_for_skin_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where suitable_for_skin_type is not null
 union all
select goods_id,'single_color_multi_color' as attr_key,single_color_multi_color as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where single_color_multi_color is not null
 union all
select goods_id,'type_of_gloss' as attr_key,type_of_gloss as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where type_of_gloss is not null
 union all
select goods_id,'net_weight' as attr_key,net_weight as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where net_weight is not null
 union all
select goods_id,'one_time' as attr_key,one_time as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where one_time is not null
 union all
select goods_id,'color_of_lace' as attr_key,color_of_lace as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where color_of_lace is not null
 union all
select goods_id,'battery_type' as attr_key,battery_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where battery_type is not null
 union all
select goods_id,'layer' as attr_key,layer as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where layer is not null
 union all
select goods_id,'power' as attr_key,power as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where power is not null
 union all
select goods_id,'certification' as attr_key,certification as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where certification is not null
 union all
select goods_id,'manufacturing_process' as attr_key,manufacturing_process as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where manufacturing_process is not null
 union all
select goods_id,'shape' as attr_key,shape as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where shape is not null
 union all
select goods_id,'smell' as attr_key,smell as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where smell is not null
 union all
select goods_id,'voltage' as attr_key,voltage as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where voltage is not null
 union all
select goods_id,'hair_type' as attr_key,hair_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where hair_type is not null
 union all
select goods_id,'diameter' as attr_key,diameter as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where diameter is not null
 union all
select goods_id,'service_life' as attr_key,service_life as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where service_life is not null
 union all
select goods_id,'number_of_pieces' as attr_key,number_of_pieces as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where number_of_pieces is not null
 union all
select goods_id,'features' as attr_key,features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where features is not null
 union all
select goods_id,'washing_mode' as attr_key,washing_mode as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where washing_mode is not null
 union all
select goods_id,'wigs_length' as attr_key,wigs_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where wigs_length is not null
 union all
select goods_id,'specifications' as attr_key,specifications as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where specifications is not null
 union all
select goods_id,'material_of_heat_conduction_board' as attr_key,material_of_heat_conduction_board as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where material_of_heat_conduction_board is not null
 union all
select goods_id,'blade_material' as attr_key,blade_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where blade_material is not null
 union all
select goods_id,'working_principle' as attr_key,working_principle as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where working_principle is not null
 union all
select goods_id,'handle_material' as attr_key,handle_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where handle_material is not null
 union all
select goods_id,'plug_type' as attr_key,plug_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_health_beauty where plug_type is not null
) a;
insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,14 as cat_attr_id,a.second_cat_id from (
select goods_id,'age' as attr_key,age as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where age is not null
 union all
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where material is not null
 union all
select goods_id,'color' as attr_key,color as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where color is not null
 union all
select goods_id,'gender' as attr_key,gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where gender is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where style is not null
 union all
select goods_id,'feature' as attr_key,feature as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where feature is not null
 union all
select goods_id,'pattern_type' as attr_key,pattern_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where pattern_type is not null
 union all
select goods_id,'season' as attr_key,season as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where season is not null
 union all
select goods_id,'product_type' as attr_key,product_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where product_type is not null
 union all
select goods_id,'sleeve_length' as attr_key,sleeve_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where sleeve_length is not null
 union all
select goods_id,'age_range' as attr_key,age_range as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where age_range is not null
 union all
select goods_id,'height' as attr_key,height as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where height is not null
 union all
select goods_id,'thickness' as attr_key,thickness as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where thickness is not null
 union all
select goods_id,'collar' as attr_key,collar as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where collar is not null
 union all
select goods_id,'decoration' as attr_key,decoration as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where decoration is not null
 union all
select goods_id,'function' as attr_key,function as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where function is not null
 union all
select goods_id,'material_feature' as attr_key,material_feature as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where material_feature is not null
 union all
select goods_id,'upper_material' as attr_key,upper_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where upper_material is not null
 union all
select goods_id,'outsole_material' as attr_key,outsole_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where outsole_material is not null
 union all
select goods_id,'brand' as attr_key,brand as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where brand is not null
 union all
select goods_id,'capacity' as attr_key,capacity as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where capacity is not null
 union all
select goods_id,'dress_length' as attr_key,dress_length as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where dress_length is not null
 union all
select goods_id,'shoe_size' as attr_key,shoe_size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where shoe_size is not null
 union all
select goods_id,'main_material' as attr_key,main_material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_baby_stuff where main_material is not null
) a;
insert into table ads.ads_vova_goods_pre_attribute_data
select a.goods_id,a.attr_key,a.attr_value,15 as cat_attr_id,a.second_cat_id from (
select goods_id,'material' as attr_key,material as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where material is not null
 union all
select goods_id,'features' as attr_key,features as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where features is not null
 union all
select goods_id,'suitable_age' as attr_key,suitable_age as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where suitable_age is not null
 union all
select goods_id,'size' as attr_key,size as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where size is not null
 union all
select goods_id,'toy_types' as attr_key,toy_types as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where toy_types is not null
 union all
select goods_id,'occation' as attr_key,occation as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where occation is not null
 union all
select goods_id,'color' as attr_key,color as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where color is not null
 union all
select goods_id,'shape' as attr_key,shape as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where shape is not null
 union all
select goods_id,'interest' as attr_key,interest as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where interest is not null
 union all
select goods_id,'age_range' as attr_key,age_range as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where age_range is not null
 union all
select goods_id,'gender' as attr_key,gender as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where gender is not null
 union all
select goods_id,'pattern_type' as attr_key,pattern_type as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where pattern_type is not null
 union all
select goods_id,'style' as attr_key,style as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where style is not null
 union all
select goods_id,'brand' as attr_key,brand as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where brand is not null
 union all
select goods_id,'battery' as attr_key,battery as attr_value,cat_id as second_cat_id from dwd.dwd_vova_goods_attribute_toys_games where battery is not null
) a;
"
spark-sql --conf "spark.app.name=ads_vova_goods_pre_attribute_data_increase" --conf "spark.dynamicAllocation.maxExecutors=10" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi