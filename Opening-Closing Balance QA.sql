/*
- Viz: https://docs.google.com/spreadsheets/d/1oVcJlLbvdTg09g5EFzcKfig271A6c6vX9gj516QTdHw/edit#gid=0
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

-- closings not found against openings
select distinct tbl1.tallykhata_user_id, opening_balance_date, closing_balance_date, version_updated_at, opening_balance_created_at balance_created_at
from 
	(select tallykhata_user_id, date(input_date) opening_balance_date, created_at::timestamp opening_balance_created_at
	from public.sync_openingbalance
	) tbl1 
	
	left join 
	
	(select distinct tallykhata_user_id, date(closing_date) closing_balance_date
	from public.sync_closingbalance
	) tbl2 on(tbl1.tallykhata_user_id=tbl2.tallykhata_user_id and closing_balance_date=opening_balance_date-1)
	
	inner join 
	
	(select tallykhata_user_id, max(updated_at) version_updated_at
	from public.registered_users 
	where app_version_name='4.0.2'
	group by 1
	) tbl3 on(tbl1.tallykhata_user_id=tbl3.tallykhata_user_id)
where 
	1=1
	and opening_balance_created_at>=version_updated_at	
	and closing_balance_date is null
	
union all

-- openings not found against closings
select distinct tbl1.tallykhata_user_id, opening_balance_date, closing_balance_date, version_updated_at, closing_balance_created_at balance_created_at
from 
	(select tallykhata_user_id, date(closing_date) closing_balance_date, created_at::timestamp closing_balance_created_at
	from public.sync_closingbalance
	) tbl1 
	
	left join 
	
	(select distinct tallykhata_user_id, date(input_date) opening_balance_date
	from public.sync_openingbalance
	) tbl2 on(tbl1.tallykhata_user_id=tbl2.tallykhata_user_id and closing_balance_date=opening_balance_date-1)
	
	inner join 
	
	(select tallykhata_user_id, max(updated_at) version_updated_at
	from public.registered_users 
	where app_version_name='4.0.2'
	group by 1
	) tbl3 on(tbl1.tallykhata_user_id=tbl3.tallykhata_user_id)
where 
	1=1
	and closing_balance_created_at>=version_updated_at	
	and opening_balance_date is null; 

-- for corresponding phone numbers and registration date
select *
from 
	(select distinct tallykhata_user_id, mobile mobile_no
	from public.registered_users 
	where tallykhata_user_id in 
		(270926,
		817696,
		819554,
		997937,
		1094173,
		1236319,
		1733623,
		2161766,
		2372311,
		2698510,
		2949406,
		656897,
		2140800)
	) tbl1
	
	inner join 
		
	(select tallykhata_user_id, max(updated_at) version_updated_at
	from public.registered_users 
	where app_version_name='4.0.2'
	group by 1
	) tbl2 using(tallykhata_user_id) 
	
	inner join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	) tbl3 using(mobile_no); 

