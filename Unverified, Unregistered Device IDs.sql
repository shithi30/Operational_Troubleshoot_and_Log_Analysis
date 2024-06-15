/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1314719952
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Need unregistered and unverified user data on daily basis
- Notes (if any): shared as .csvs
*/

-- unregistered
drop table if exists data_vajapora.unregistered_device_id; 
create table data_vajapora.unregistered_device_id as
select distinct tbl1.device_id
from 
	(select device_id 
	from public.notification_fcmtoken 
	where app_status='ACTIVE'
	) tbl1 
	
	left join 
	
	(select device_id 
	from public.registered_users 
	) tbl2 using(device_id)
	
	left join 
	
	(select device_id 
	from public.register_unverifieduserapp
	) tbl3 using(device_id)
where 
	tbl2.device_id is null
	and tbl3.device_id is null; 

select * 
from data_vajapora.unregistered_device_id; 

-- unverified
drop table if exists data_vajapora.unverified_device_id; 
create table data_vajapora.unverified_device_id as
select distinct tbl1.device_id
from 
	(select device_id 
	from public.notification_fcmtoken 
	where app_status='ACTIVE'
	) tbl1 
	
	left join 
	
	(select device_id 
	from public.registered_users 
	) tbl2 using(device_id)
	
	left join 
	
	(select device_id 
	from public.register_unverifieduserapp
	) tbl3 using(device_id)
where 
	tbl2.device_id is null
	and tbl3.device_id is not null; 

select * 
from data_vajapora.unverified_device_id;

/* Version-02 */

-- device_id+token
drop table if exists data_vajapora.unreg_unv_help; 
create table data_vajapora.unreg_unv_help as
select 
	tbl1.device_id fcmtoken_device_id, 
	tbl2.device_id registered_device_id, 
	tbl3.device_id unverified_device_id, 
	token fcmtoken
from 
	(select device_id, token
	from public.notification_fcmtoken 
	where app_status='ACTIVE'
	) tbl1 
	
	left join 
	
	(select distinct device_id
	from public.registered_users 
	) tbl2 using(device_id)
	
	left join 
	
	(select distinct device_id 
	from public.register_unverifieduserapp
	) tbl3 using(device_id); 

-- unregistered device_id
select distinct fcmtoken_device_id
from 
	(select fcmtoken_device_id, fcmtoken
	from data_vajapora.unreg_unv_help
	where 
		registered_device_id is null
		and unverified_device_id is null
	) tbl1 
	
	left join 
		
	(-- registered token
	select fcmtoken 
	from data_vajapora.unreg_unv_help
	where registered_device_id is not null
	) tbl2 using(fcmtoken)
where tbl2.fcmtoken is null; 

-- unverified device_id
select distinct fcmtoken_device_id
from 
	(select fcmtoken_device_id, fcmtoken
	from data_vajapora.unreg_unv_help
	where 
		registered_device_id is null
		and unverified_device_id is not null
	) tbl1 
	
	left join 
		
	(-- registered token
	select fcmtoken 
	from data_vajapora.unreg_unv_help
	where registered_device_id is not null
	) tbl2 using(fcmtoken)
where tbl2.fcmtoken is null; 



