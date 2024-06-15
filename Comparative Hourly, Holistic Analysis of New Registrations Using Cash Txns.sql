/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1664237160
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): Is the cash txn record tendency is high in version 4 neonatal compared to version 3 neonatal?
*/

-- comparative hourly analysis 
select reg_date, reg_hour, concat(reg_date, ' hour: ', reg_hour) reg_date_hour, new_version_registrations, new_version_registrations_used_cash, old_version_registrations, old_version_registrations_used_cash
from 
	(-- new version registrations
	select 
		date(reg_datetime) reg_date,
		date_part('hour', reg_datetime) reg_hour,
		count(tbl1.mobile_no) new_version_registrations,
		count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) new_version_registrations_used_cash
	from 
		(-- all new registrations in new release
		select mobile_number mobile_no, created_at reg_datetime
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
		where tbl_1.created_at >= '2021-09-29 18:52:00'
		) tbl1 
		
		left join 
		
		(-- used cash after new release
		select distinct mobile_no
		from public.journal
		where 
			txn_type in(1, 2, 6, 7, 8)
			and txn_mode=1
			and create_date>='2021-09-29 18:52:00'
		) tbl2 using(mobile_no)
	group by 1, 2
	) tbl1
	
	inner join 
	
	(-- old version registrations
	select 
		date(reg_datetime) reg_date,
		date_part('hour', reg_datetime) reg_hour,
		count(tbl1.mobile_no) old_version_registrations,
		count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) old_version_registrations_used_cash
	from 
		(-- all new registrations in new release
		select mobile_number mobile_no, created_at reg_datetime
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
			    u.app_version_number < 99 and lower(u.device_status) = 'active'
			) tbl_1 
		where tbl_1.created_at >= '2021-09-29 18:52:00'
		) tbl1 
		
		left join 
		
		(-- used cash after new release
		select distinct mobile_no
		from public.journal
		where 
			txn_type in(1, 2, 6, 7, 8)
			and txn_mode=1
			and create_date>='2021-09-29 18:52:00'
		) tbl2 using(mobile_no)
	group by 1, 2
	) tbl2 using(reg_date, reg_hour)
order by 1, 2; 

-- comparative combined analysis 
select 
	sum(new_version_registrations) new_version_registrations,
	sum(new_version_registrations_used_cash) new_version_registrations_used_cash,
	sum(old_version_registrations) old_version_registrations,
	sum(old_version_registrations_used_cash) old_version_registrations_used_cash
from 
	(-- new version registrations
	select 
		date(reg_datetime) reg_date,
		date_part('hour', reg_datetime) reg_hour,
		count(tbl1.mobile_no) new_version_registrations,
		count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) new_version_registrations_used_cash
	from 
		(-- all new registrations in new release
		select mobile_number mobile_no, created_at reg_datetime
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
		where tbl_1.created_at >= '2021-09-29 18:52:00'
		) tbl1 
		
		left join 
		
		(-- used cash after new release
		select distinct mobile_no
		from public.journal
		where 
			txn_type in(1, 2, 6, 7, 8)
			and txn_mode=1
			and create_date>='2021-09-29 18:52:00'
		) tbl2 using(mobile_no)
	group by 1, 2
	) tbl1
	
	inner join 
	
	(-- old version registrations
	select 
		date(reg_datetime) reg_date,
		date_part('hour', reg_datetime) reg_hour,
		count(tbl1.mobile_no) old_version_registrations,
		count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) old_version_registrations_used_cash
	from 
		(-- all new registrations in new release
		select mobile_number mobile_no, created_at reg_datetime
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
			    u.app_version_number < 99 and lower(u.device_status) = 'active'
			) tbl_1 
		where tbl_1.created_at >= '2021-09-29 18:52:00'
		) tbl1 
		
		left join 
		
		(-- used cash after new release
		select distinct mobile_no
		from public.journal
		where 
			txn_type in(1, 2, 6, 7, 8)
			and txn_mode=1
			and create_date>='2021-09-29 18:52:00'
		) tbl2 using(mobile_no)
	group by 1, 2
	) tbl2 using(reg_date, reg_hour); 

