select 
	date(updated_at) "date",
	date_part('hour', updated_at) "hour",
	quarter_of_hour,
    -- date of release
    count(case when tbl_1.created_at < '2021-09-29 18:52:00' then tbl_1.mobile_number end) as total_updated_user,
    count(case when tbl_1.created_at >='2021-09-29 18:52:00' then tbl_1.mobile_number end) as total_newly_registered_user
from    
	(select
	    m.mobile_number,
	    u.app_version_name,
	    u.app_version_number,
	    m.created_at,
	    u.updated_at,
	    case 
	    	when date_part('minute', u.updated_at)<15 then '1st quarter of hour'
	    	when date_part('minute', u.updated_at)<30 then '2nd quarter of hour'
	    	when date_part('minute', u.updated_at)<45 then '3rd quarter of hour'
	    	when date_part('minute', u.updated_at)<=59 then '4th quarter of hour'
	    end quarter_of_hour
	from
	    public.registered_users as u
	inner join public.register_usermobile as m on
	    u.mobile = m.mobile_number
	where
	    u.app_version_number >= 99 and lower(u.device_status) = 'active' -- new version number
	) tbl_1 
group by 1, 2, 3
order by 1 desc, 2 desc, 3 desc;