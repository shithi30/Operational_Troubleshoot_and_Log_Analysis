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
- Email thread: Unregistered GAIDs
- Notes (if any): 
	- How many GAID we have? Do we have any data of not registered merchants GAID?
	We have 2,088,315 GAIDs in total. 
	Among them, 1,443,542 (~70%) correspond to registered users. 
	The rest 644,773 (~30%) are representative of unregistered/unverified user-base.
*/

-- GAID metrics
select 
	*,
	ad_ids_unregistered*1.00/ad_ids ad_ids_unregistered_pct, 
	ad_ids_registered*1.00/ad_ids ad_ids_registered_pct
from 
	(select 
		count(tbl1.advertise_id) ad_ids, 
		count(case when tbl2.advertise_id is null then tbl1.advertise_id else null end) ad_ids_unregistered, 
		count(case when tbl2.advertise_id is not null then tbl1.advertise_id else null end) ad_ids_registered
	from 
		(select distinct advertise_id
		from public.google_ad_id
		) tbl1 
		
		left join 
		
		(select distinct advertise_id
		from public.register_tallykhatauser
		where mobile_no is not null 
		) tbl2 using(advertise_id)
	) tbl1; 

-- unregistered GAIDs	
select tbl1.advertise_id
from 
	(select distinct advertise_id
	from public.google_ad_id
	) tbl1 
	
	left join 
	
	(select distinct advertise_id
	from public.register_tallykhatauser
	where mobile_no is not null 
	) tbl2 using(advertise_id)
where tbl2.advertise_id is null; 

-- date-wise users (and their GAIDs) registered
select 
	reg_date, 
	count(distinct mobile_no) users_registered, 
	count(distinct advertise_id) unique_gaids_registered, 
	count(advertise_id) gaids_registered 
from 
	data_vajapora.unregistered_gaids tbl1 -- imported
	
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

	