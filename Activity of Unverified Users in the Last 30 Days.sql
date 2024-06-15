/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=335360683
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Presentation: 
- Email thread: 
- Notes (if any): 
	Nazrul bhai, need the following data on unverified users.
	1. Percentage of users recorded >10  transactions in last 30 days
	2. Percentage of users recorded >20  transactions in last 30 days
	3. Percentage of users added >3  customers in last 30 days
*/

-- yet unverified users
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select tbl1.mobile_no, id app_id
from 
	(select mobile mobile_no, id
	from public.register_unverifieduserapp 
	-- where date(created_at)>=current_date-30 and date(created_at)<current_date
	) tbl1 
	
	left join 
	
	(select distinct mobile_number mobile_no
	from public.register_usermobile 
	) tbl2 using(mobile_no)
where tbl2.mobile_no is null; 

-- summary metrics
select 
	count(distinct tbl1.mobile_no) unverified_users, 
	
	count(distinct case when tbl2.txn_adds>10 then tbl1.mobile_no else null end) greater_than_10_txns_last_30_days,
	count(distinct case when tbl2.txn_adds>20 then tbl1.mobile_no else null end) greater_than_20_txns_last_30_days, 
	count(distinct case when tbl3.cust_adds>3 then tbl1.mobile_no else null end) greater_than_3_custs_last_30_days,
	
	count(distinct case when tbl2.txn_adds>10 then tbl1.mobile_no else null end)*1.00/count(distinct tbl1.mobile_no) greater_than_10_txns_last_30_days_pct,
	count(distinct case when tbl2.txn_adds>20 then tbl1.mobile_no else null end)*1.00/count(distinct tbl1.mobile_no) greater_than_20_txns_last_30_days_pct, 
	count(distinct case when tbl3.cust_adds>3 then tbl1.mobile_no else null end)*1.00/count(distinct tbl1.mobile_no) greater_than_3_custs_last_30_days_pct
from 
	data_vajapora.help_a tbl1
	
	left join 

	(-- unverified users' last 30 days' txns
	select app_id, count(case when txn_cat is not null then txn_id else null end) txn_adds
	from 
		(select 
			app_id, 
			id txn_id,
			case 
				when txn_type=1 and txn_mode=1 then 'CASH_SALE'
				when txn_type=2 and txn_mode=1 then 'CASH_PURCHASE'
				when txn_type=5 and txn_mode=1 then 'EXPENSE'
				when txn_type=3 and txn_mode=1 and coalesce(amount_received,0)>0 then 'CREDIT_SALE_RETURN'
				when txn_type=4 and txn_mode=1 and coalesce(amount_received,0)>0 then 'CREDIT_PURCHASE' 
				when txn_type=3 and txn_mode=1 and coalesce(amount,0)>0 then 'CREDIT_SALE'
				when txn_type=4 and txn_mode=1 and coalesce(amount,0)>0 then 'CREDIT_PURCHASE_RETURN'
				when txn_type=3 and txn_mode=2 then 'DIGITAL_SALE'
			end txn_cat
		from public.sync_unverifieduserjournal
		where date(create_date)>=current_date-30 and date(create_date)<current_date 
		) tbl1
	group by 1
	) tbl2 on(tbl1.app_id=tbl2.app_id)

	left join 

	(-- unverified users' last 30 days' cust-adds
	select app_id, count(id) cust_adds
	from public.sync_unverifieduseraccount 
	where 
		type=2
		and date(create_date)>=current_date-30 and date(create_date)<current_date 
	group by 1
	) tbl3 on(tbl1.app_id=tbl3.app_id); 
	