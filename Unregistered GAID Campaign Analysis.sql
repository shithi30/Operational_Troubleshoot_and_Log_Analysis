/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1002800918
	- https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=710709922
- Data: 
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Unregistered GAIDs
- Notes (if any): 
	phase-02: data_vajapora.unregistered_gaids_2 
*/

-- GAIDs registered	
select 
	count(tbl1.advertise_id) gaids_initially_unregistered, 
	count(tbl2.advertise_id) gaids_now_registered, 
	count(tbl2.advertise_id)*1.00/count(tbl1.advertise_id) gaids_now_registered_pct
from 
	data_vajapora.unregistered_gaids tbl1 -- imported
	
	left join 
	
	(select distinct advertise_id
	from public.register_tallykhatauser
	where mobile_no is not null 
	) tbl2 using(advertise_id); 

-- date-wise GAIDs registered
select date(updated_at) reg_date, count(distinct advertise_id) gaids_registered, count(distinct mobile_no) merchants_registered
from 
	(-- imported
	select * from data_vajapora.unregistered_gaids 
	union 
	select * from data_vajapora.unregistered_gaids_2
	) tbl1 
	
	inner join 
	
	(select advertise_id, mobile_no, max(updated_at) updated_at
	from public.register_tallykhatauser
	where mobile_no is not null 
	group by 1, 2
	) tbl2 using(advertise_id)
group by 1
order by 1; 

-- date-wise users (and their GAIDs) registered
select 
	reg_date, 
	count(distinct mobile_no) users_registered, 
	count(distinct advertise_id) gaids_registered
from 
	(-- imported
	select * from data_vajapora.unregistered_gaids 
	union 
	select * from data_vajapora.unregistered_gaids_2
	) tbl1 
	
	inner join 
	
	(select advertise_id, mobile_no
	from public.register_tallykhatauser
	where mobile_no is not null 
	) tbl2 using(advertise_id)
	
	left join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile
	) tbl3 using(mobile_no)
group by 1
order by 1; 

-- registered later
select reg_date, count(distinct mobile_no) registered_later
from 
	(select device_id, date(created_at) installation_date
	from public.notification_fcmtoken
	) tbl1 
	
	inner join 
	
	(select device_id, mobile mobile_no 
	from public.registered_users 
	) tbl2 using(device_id)
	
	inner join 
	
	(select mobile_number mobile_no, date(created_at) reg_date
	from public.register_usermobile
	) tbl3 using(mobile_no)
where reg_date>installation_date
group by 1; 
	
-- comparison with Sanjida Apu's campaigns
select *
from 
	(-- date-wise users registered
	select date(created_at) reg_date, count(mobile_number) merchants_registered
	from public.register_usermobile
	group by 1
	) tbl0
	
	inner join 
	
	(-- date-wise users (from unregistered device_ids) registered
	select 
		reg_date, 
		count(distinct mobile_no) merchants_registered_deviceid
	from 
		(select device_id 
		from data_vajapora."unregistered_28-Feb-22"
		) tbl1 
		
		inner join 
		
		(select device_id, mobile mobile_no
		from public.registered_users
		) tbl2 using(device_id)
		
		inner join
		
		(select mobile_number mobile_no, date(created_at) reg_date
		from public.register_usermobile
		) tbl3 using(mobile_no)
	group by 1
	) tbl1 using(reg_date)
	
	inner join 
	
	(-- date-wise users (from unregistered GAIDs) registered
	select date(updated_at) reg_date, count(distinct mobile_no) merchants_registered_gaid
	from 
		(-- imported
		select * from data_vajapora.unregistered_gaids 
		union 
		select * from data_vajapora.unregistered_gaids_2
		) tbl1 
		
		inner join 
		
		(select advertise_id, mobile_no, max(updated_at) updated_at
		from public.register_tallykhatauser
		where mobile_no is not null 
		group by 1, 2
		) tbl2 using(advertise_id)
	group by 1
	) tbl2 using(reg_date)
order by 1; 

