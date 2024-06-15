/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): Hourly auto emails were shot to marketing with the 2 csvs. 
*/

-- for new registrations
select mobile_number, app_version_name, app_version_number, created_at registered_at  
from    
	(select
	    m.mobile_number,
	    u.app_version_name,
	    u.app_version_number,
	    m.created_at,
	    u.updated_at
	from
	    public.registered_users as u
	inner join public.register_usermobile as m on
	    u.mobile = m.mobile_number
	where
	    u.app_version_number >= 99 and lower(u.device_status) = 'active'
	) tbl_1 
where 
	updated_at>=now()-interval '1 hours' and updated_at<now()
	and tbl_1.created_at >= '2021-09-29 18:52:00'; 

-- for updates
select mobile_number, app_version_name, app_version_number, updated_at   
from    
	(select
	    m.mobile_number,
	    u.app_version_name,
	    u.app_version_number,
	    m.created_at,
	    u.updated_at
	from
	    public.registered_users as u
	inner join public.register_usermobile as m on
	    u.mobile = m.mobile_number
	where
	    u.app_version_number >= 99 and lower(u.device_status) = 'active'
	) tbl_1 
where 
	updated_at>=now()-interval '1 hours' and updated_at<now()
	and tbl_1.created_at < '2021-09-29 18:52:00'; 