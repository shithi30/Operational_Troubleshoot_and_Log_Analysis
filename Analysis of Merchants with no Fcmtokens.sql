/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1239728349
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

-- unknown reg merchants
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select tbl1.mobile_no
from 
	(-- reg merchants
	select mobile_number mobile_no 
	from public.register_usermobile 
	where created_at::date<current_date
	) tbl1 
	
	left join 
	
	(-- reg merchants with fcmtokens
	select distinct mobile_no 
	from 
		(select mobile mobile_no, tallykhata_user_id, device_id
		from public.registered_users 
		) tbl1 
		
		inner join 
		
		(select device_id
		from public.notification_fcmtoken
		) tbl2 using(device_id)
	) tbl2 using(mobile_no)
where tbl2.mobile_no is null; 

-- biz types by Mahmud
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select 
	mobile_no, 
	case 
		when business_type in('BAKERY_AND_CONFECTIONERY') then 'SWEETS AND CONFECTIONARY'
		when business_type in('ELECTRONICS') then 'ELECTRONICS STORE'
		when business_type in('MFS_AGENT','MFS_MOBILE_RECHARGE') then 'MFS-MOBILE RECHARGE STORE'
		when business_type in('GROCERY') then 'GROCERY'
		when business_type in('DISTRIBUTOR_OR_WHOLESALE','WHOLESALER','DEALER') then 'OTHER WHOLESELLER'
		when business_type in('HOUSEHOLD_AND_FURNITURE') then 'FURNITURE SHOP'
		when business_type in('STATIONERY') then 'STATIONARY BUSINESS'
		when business_type in('TAILORS') then 'TAILERS'
		when business_type in('PHARMACY') then 'PHARMACY'
		when business_type in('SHOE_STORE') then 'SHOE STORE'
		when business_type in('MOTOR_REPAIR') then 'VEHICLE-CAR SERVICING'
		when business_type in('COSMETICS') then 'COSMETICS AND PERLOUR'
		when business_type in('ROD_CEMENT') then 'CONSTRUCTION RAW MATERIAL'
		when business_type='' then upper(case when new_bi_business_type!='Other Business' then new_bi_business_type else null end) 
		else null 
	end biz_type
from 
	(select id, business_type 
	from public.register_tallykhatauser 
	) tbl1 
	
	inner join 
	
	(select mobile_no, max(id) id
	from public.register_tallykhatauser 
	group by 1
	) tbl2 using(id)
	
	inner join 
	
	(select 
		mobile mobile_no, 
		max(new_bi_business_type) new_bi_business_type
	from tallykhata.tallykhata_user_personal_info 
	group by 1
	) tbl3 using(mobile_no); 

-- TG, biz type wise distribution 
select
	case 
		when tg in('3RAUCb','3RAU Set-A','3RAU Set-B','3RAU Set-C','3RAUTa','3RAUTa+Cb','3RAUTacs') then '3RAU'
		when tg in('LTUCb','LTUTa') then 'LTU'
		when tg in('NT--') then 'NT'
		when tg in('NB0','NN1','NN2-6') then 'NN'
		when tg in('PSU') then 'PSU'
		when tg in('PUCb','PU Set-A','PU Set-B','PU Set-C','PUTa','PUTa+Cb','PUTacs') then 'PU'
		when tg in('ZCb','ZTa','ZTa+Cb') then 'Zombie' 
		else 'rest'
	end segment, 
	biz_type, 
	count(mobile_no) unknown_merchants
from 
	data_vajapora.help_a tbl1 
	
	left join 
	
	(select mobile_no, max(tg) tg
	from cjm_segmentation.retained_users 
	where report_date=current_date
	group by 1
	) tbl2 using(mobile_no)
	
	left join 
	
	data_vajapora.help_b tbl3 using(mobile_no)
group by 1, 2; 

-- distribution of zombie
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
/*select mobile_no, min(report_date) min_zombie_date 
from 
	data_vajapora.help_a tbl1 
	
	left join 

	(select mobile_no, report_date 
	from cjm_segmentation.retained_users 
	where tg in('ZCb','ZTa','ZTa+Cb')
	) tbl2 using(mobile_no)
group by 1;*/
select mobile_no, max(event_date) min_zombie_date 
from 
	data_vajapora.help_a tbl1 
	
	left join 

	(select mobile_no, event_date
	from tallykhata.tallykhata_user_date_sequence_final 
	) tbl2 using(mobile_no)
group by 1;

select 
	case
		when min_zombie_date is null then 'no events found'
		when current_date-min_zombie_date<=7 then 'zombie within 07 days' 
		when current_date-min_zombie_date>07 and current_date-min_zombie_date<=14 then 'zombie within 08 to 14 days' 
		when current_date-min_zombie_date>14 and current_date-min_zombie_date<=21 then 'zombie within 15 to 21 days' 
		when current_date-min_zombie_date>21 and current_date-min_zombie_date<=28 then 'zombie within 22 to 28 days' 
		when current_date-min_zombie_date>28 and current_date-min_zombie_date<=35 then 'zombie within 29 to 35 days' 
		when current_date-min_zombie_date>35 and current_date-min_zombie_date<=42 then 'zombie within 36 to 42 days' 
		when current_date-min_zombie_date>42 and current_date-min_zombie_date<=49 then 'zombie within 43 to 49 days'
		else 'zombie 50 or more days ago'
	end zombie_cat, 
	count(mobile_no) zombie_unknown_merchants
from data_vajapora.help_c
group by 1; 

-- distribution of version
select 
	case when app_version is null then 'uninstalled' else app_version end app_version, 
	count(mobile_no) unknown_merchants
from 
	data_vajapora.help_a tbl1 
	
	left join 
	
	(select mobile_no, max(app_version) app_version
	from cjm_segmentation.retained_users 
	where report_date=current_date-1
	group by 1
	) tbl2 using(mobile_no) 
group by 1
order by 1 desc; 
	
-- distribution of msg receive
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select mobile_no, max(event_date) last_msg_received  
from 
	data_vajapora.help_a tbl1 
	
	left join 

	(select mobile_no, event_date 
	from tallykhata.tallykhata_sync_event_fact_final
	where event_name='inbox_message_received'
	) tbl2 using(mobile_no)
group by 1;

select 
	case 
		when last_msg_received is null then 'never received msg'
		when current_date-last_msg_received<=7 then 'received within 07 days' 
		when current_date-last_msg_received>07 and current_date-last_msg_received<=14 then 'received within 08 to 14 days' 
		when current_date-last_msg_received>14 and current_date-last_msg_received<=21 then 'received within 15 to 21 days' 
		when current_date-last_msg_received>21 and current_date-last_msg_received<=28 then 'received within 22 to 28 days' 
		when current_date-last_msg_received>28 and current_date-last_msg_received<=35 then 'received within 29 to 35 days' 
		when current_date-last_msg_received>35 and current_date-last_msg_received<=42 then 'received within 36 to 42 days' 
		when current_date-last_msg_received>42 and current_date-last_msg_received<=49 then 'received within 43 to 49 days'
		else 'received 50 or more days ago'
	end rec_cat, 
	count(mobile_no) rec_unknown_merchants
from data_vajapora.help_d
group by 1; 

-- today's TG distribution of version
select app_version, count(distinct mobile_no) today_tg_count
from cjm_segmentation.retained_users  
where report_date=current_date
group by 1
order by 1 desc;
