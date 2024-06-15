/*
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1314719952
- Notes: 
	I need brief numbers for the following queries:
		- How many total tokens we have
		- How many retained
		- How many unverified
		- How many unregistered
		- How many unverified retained
		- How many unregistered retained
		- How many users are from below v2.5?
	        - uninstalls tracked for all generated tokens currently including unverified/unregistered?
	- Masum Bhai notes: https://docs.google.com/presentation/d/1KeQYHYqBA_bpXQU2990k1nfA1dSih8eiOZmeWKgtEIo/edit#slide=id.p
*/

drop table if exists data_vajapora.fcm_help_a; 
create table data_vajapora.fcm_help_a as
select device_id, token, app_version_number, updated_at app_status_updated_at, app_status
from  
	(select id, device_id, token, app_version_number, updated_at, app_status
	from public.notification_fcmtoken 
	) tbl1 
	
	inner join 
	
	(select token, max(id) id 
	from public.notification_fcmtoken  
	group by 1
	) tbl2 using(token, id); 

drop table if exists data_vajapora.fcm_help_b; 
create table data_vajapora.fcm_help_b as
select mobile_no, device_id, device_created_at
from 
	(select id, device_id, mobile mobile_no, created_at device_created_at
    from public.registered_users 
    ) tbl1

    inner join 

    (select mobile mobile_no, max(id) id
    from public.registered_users 
    where device_status='active'
    group by 1
    ) tbl2 using(id, mobile_no); 

select 
	count(distinct token) total_tokens, 
	count(distinct case when mobile_no is not null and app_status='ACTIVE' then token else null end) tokens_retained, 
	count(distinct case when mobile_no is not null and app_status='UNINSTALLED' then token else null end) tokens_uninstalled_after_registration, 
	count(distinct case when mobile_no is null and app_status='ACTIVE' then token else null end) tokens_unverified_unregistered,
	count(distinct case when mobile_no is null and app_status='ACTIVE' and unverified_device_id is not null then token else null end) tokens_unverified,
	count(distinct case when mobile_no is null and app_status='ACTIVE' and unverified_device_id is null then token else null end) tokens_unregistered,
	count(distinct case when mobile_no is null and app_status='UNINSTALLED' then token else null end) tokens_uninstalled_before_registration
from 
	(select  
		token, 
	    mobile_no,
	    device_id,
	    device_created_at,
	    app_status, 
	    app_status_updated_at, 
	    tbl4.device_id unverified_device_id
	from
		data_vajapora.fcm_help_a tbl1 

		left join 
	
		data_vajapora.fcm_help_b tbl2 using(device_id)
		
		left join 
		
		(select distinct device_id 
		from public.register_unverifieduserapp
		) tbl4 using(device_id)
	) tbl1; 

select count(distinct mobile_no) merchants_below_81
from 
	(select id, device_id, mobile mobile_no, created_at device_created_at, app_version_number
    from public.registered_users 
    ) tbl1

    inner join 

    (select mobile mobile_no, max(id) id
    from public.registered_users 
    where device_status='active'
    group by 1
    ) tbl2 using(id, mobile_no)
where app_version_number<81; 

select count(distinct token) tokens_retained_in_inactive_devices
from 
	data_vajapora.fcm_help_a tbl1 
	inner join 
	(select device_id, device_status
	from public.registered_users 
	) tbl2 using(device_id)
where 
	app_status='ACTIVE'
	and device_status='inactive'; 
