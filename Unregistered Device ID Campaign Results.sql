/*
- Viz: 
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Need unregistered and unverified user data on daily basis
- Notes (if any): 
*/

-- device_id counts
select 
	count(distinct tbl1.device_id) total_device_id, 
	count(distinct case when tbl1.app_status='UNINSTALLED' then tbl1.device_id else null end) uninstalled_device_id, 
	
	count(distinct case when tbl1.app_status='ACTIVE' and tbl2.device_id is not null then tbl1.device_id else null end) registered_device_id,
	count(distinct case when tbl1.app_status='ACTIVE' and tbl2.device_id is not null and device_status='active' then tbl1.device_id else null end) registered_active_device_id,                   
	count(distinct case when tbl1.app_status='ACTIVE' and tbl2.device_id is not null and device_status='inactive' then tbl1.device_id else null end) registered_inactive_device_id,                   
	
	count(distinct case when tbl1.app_status='ACTIVE' and tbl2.device_id is null and tbl3.device_id is null then tbl1.device_id else null end) unregistered_device_id, 
	count(distinct case when tbl1.app_status='ACTIVE' and tbl2.device_id is null and tbl3.device_id is not null then tbl1.device_id else null end) unverified_device_id
from 
	(select device_id, app_status
	from public.notification_fcmtoken 
	) tbl1 
	
	left join 
	
	(select device_id, device_status, mobile_no
	from 
		(select device_id, device_status, mobile_no, id
		from public.registered_users 
		) tbl1 
		
		inner join 
			
		(select device_id, max(id) id 
		from public.registered_users  
		group by 1
		) tbl2 using(id, device_id)
	) tbl2 using(device_id)
	
	left join 
	
	(select device_id 
	from public.register_unverifieduserapp
	) tbl3 using(device_id); 

-- device_id labels
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select 
	*, 
	case 
		when tbl1.app_status='UNINSTALLED' then 'uninstalled_device_id'
		when tbl1.app_status='ACTIVE' and tbl2.device_id is not null and device_status='active' then 'registered_active_device_id'               
		when tbl1.app_status='ACTIVE' and tbl2.device_id is not null and device_status='inactive' then 'registered_inactive_device_id'                
		when tbl1.app_status='ACTIVE' and tbl2.device_id is null and tbl3.device_id is null then 'unregistered_device_id'
		when tbl1.app_status='ACTIVE' and tbl2.device_id is null and tbl3.device_id is not null then 'unverified_device_id'
	end device_installation_registration_status
from 
	(select device_id, app_status
	from public.notification_fcmtoken 
	) tbl1 
	
	left join 
	
	(select device_id, device_status, mobile_no
	from 
		(select device_id, device_status, mobile mobile_no, id
		from public.registered_users 
		) tbl1 
		
		inner join 
			
		(select device_id, max(id) id 
		from public.registered_users  
		group by 1
		) tbl2 using(id, device_id)
	) tbl2 using(device_id)
	
	left join 
	
	(select device_id 
	from public.register_unverifieduserapp
	) tbl3 using(device_id); 

-- see transformations
select 
	device_installation_registration_status, 
	count(device_id) device_ids, 
	count(distinct mobile_no) registered_merchants
from
	data_vajapora."unregistered_04_Mar_22" tbl1
	left join 
	data_vajapora.help_a tbl2 using(device_id)
group by 1; 
