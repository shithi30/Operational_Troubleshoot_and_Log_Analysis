/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
>> How many user has installed app in their device but not sent us location lat, lng in tk-lifetime ?
I found 45% users' loc data missing. I can't decide if this calculation is right.
*/

-- 'mobile_no's who have installed and whose devices are active
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select mobile_no, device_id, device_status, device_status_created_at
from 
	(select device_id
	from public.notification_fcmtoken 
	union 
	select device_id
	from sync_operation.notification_fcmtoken
	) tbl1 
		
	inner join 
	
	(select id, device_id, mobile mobile_no, created_at device_status_created_at, device_status
	from public.registered_users 
	where device_status='active'
	) tbl2 using(device_id)
	
	inner join 

	(select mobile mobile_no, max(id) id
	from public.registered_users 
	group by 1
	) tbl3 using(mobile_no, id); 
	
-- 45% users have loc data missing
select (count(tbl1.mobile_no)-count(tbl2.mobile_no))*1.00/count(tbl1.mobile_no) fraction_loc_unavailable
from 
	data_vajapora.help_a tbl1 
	
	left join 
	
	(-- Shovan's location table
	select distinct mobile mobile_no
	from data_vajapora.tk_users_location_sample_final
	) tbl2 using(mobile_no); 
	