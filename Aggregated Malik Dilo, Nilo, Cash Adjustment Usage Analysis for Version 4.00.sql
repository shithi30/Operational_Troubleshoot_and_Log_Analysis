/*
- Viz: https://docs.google.com/spreadsheets/d/1Cx9eTibg7oLjacqfDaJn6Y_YNYZ7Pdzswp-9pLTkZoc/edit#gid=0
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Instructions: https://docs.google.com/spreadsheets/d/1cR1WLE6aXmJx0QXvR6BR69RXPD02iPlSjj8Lk6abYLA/edit#gid=0
- Notes (if any): run on live DB
*/

-- for updates
select 
	'updated users' merchant_category,
	count(distinct tbl1.mobile_no) merchants,
	count(distinct case when txn_type=6 and txn_mode=1 then tbl1.mobile_no else null end) merchants_used_malik_nilo,
	count(distinct case when txn_type=7 and txn_mode=1 then tbl1.mobile_no else null end) merchants_used_malik_dilo,
	count(distinct case when txn_type=8 and txn_mode=1 then tbl1.mobile_no else null end) merchants_used_cash_adjustment,
	count(distinct jour_id) total_trt,
	count(distinct case when txn_type in(1, 2, 6, 7, 8) and txn_mode=1 then tbl1.mobile_no else null end) total_cash_trt,
	count(distinct acc_id) total_tacs
from 
	(select mobile_number mobile_no 
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
	where tbl_1.created_at < '2021-09-29 18:52:00' -- change here
	) tbl1 
	
	left join 
	
	(select mobile_no, id jour_id, txn_type, txn_mode
	from public.journal 
	where 
		is_active is true
		and create_date>='2021-09-29 18:52:00'
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
	
	left join 
		
	(select mobile_no, id acc_id
	from public.account 
	where 
		is_active is true 
		and type in(2, 3)
		and create_date>='2021-09-29 18:52:00'
	) tbl3 on(tbl1.mobile_no=tbl3.mobile_no) 

union all

-- for new regs
select 
	'newly registered users' merchant_category,
	count(distinct tbl1.mobile_no) merchants,
	count(distinct case when txn_type=6 and txn_mode=1 then tbl1.mobile_no else null end) merchants_used_malik_nilo,
	count(distinct case when txn_type=7 and txn_mode=1 then tbl1.mobile_no else null end) merchants_used_malik_dilo,
	count(distinct case when txn_type=8 and txn_mode=1 then tbl1.mobile_no else null end) merchants_used_cash_adjustment,
	count(distinct jour_id) total_trt,
	count(distinct case when txn_type in(1, 2, 6, 7, 8) and txn_mode=1 then tbl1.mobile_no else null end) total_cash_trt,
	count(distinct acc_id) total_tacs
from 
	(select mobile_number mobile_no 
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
	where tbl_1.created_at >= '2021-09-29 18:52:00' -- change here
	) tbl1 
	
	left join 
	
	(select mobile_no, id jour_id, txn_type, txn_mode
	from public.journal 
	where 
		is_active is true
		and create_date>='2021-09-29 18:52:00'
	) tbl2 on(tbl1.mobile_no=tbl2.mobile_no)
	
	left join 
		
	(select mobile_no, id acc_id
	from public.account 
	where 
		is_active is true 
		and type in(2, 3)
		and create_date>='2021-09-29 18:52:00'
	) tbl3 on(tbl1.mobile_no=tbl3.mobile_no); 

