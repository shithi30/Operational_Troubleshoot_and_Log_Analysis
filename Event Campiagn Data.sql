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
- Email thread: Important! Requesting For Test Campaign Data!
- Notes (if any):
*/

-- common criteria
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select * 
from 
	(select mobile_no, tg
	from cjm_segmentation.retained_users
	where 
		report_date=current_date
		and tg not like 'Z%'
	) tbl1 
	
	inner join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile
	where date(created_at)<current_date-180
	) tbl2 using(mobile_no) 
	
	inner join 
	
	(select distinct mobile_no 
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date>=current_date-30
	) tbl3 using(mobile_no);  

-- common criteria (relaxed)
drop table if exists data_vajapora.help_f; 
create table data_vajapora.help_f as
select * 
from 
	(select mobile_no, tg
	from cjm_segmentation.retained_users
	where 
		report_date=current_date
		and tg not like 'Z%'
	) tbl1 
	
	inner join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile
	where date(created_at)<current_date-90
	) tbl2 using(mobile_no) 
	
	inner join 
	
	(select distinct mobile_no 
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date>=current_date-30
	) tbl3 using(mobile_no);  
	
-- auxiliary table(s)
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select distinct mobile_no 
from tallykhata.tallykhata_sync_event_fact_final 
where 
	event_name in('manual_data_backup', 'manual_data_backup_from_menu')
	and event_date>=current_date-180; 

drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select distinct mobile_no 
from tallykhata.tallykhata_fact_info_final 
where txn_type in('CASH_PURCHASE', 'EXPENSE'); 

drop table if exists data_vajapora.baki_from_customers;
create table data_vajapora.baki_from_customers as
select
	mobile_no, 
	account_id, 
	baki+start_balance baki
from 
	(select 
		mobile_no, 
		account_id,
		sum(case when txn_type=3 and txn_mode=1 and coalesce(amount, 0)>0 then amount else 0 end)
		-
		sum(case when txn_type=3 and txn_mode=1 and coalesce(amount_received, 0)>0 then amount_received else 0 end)
		baki
	from public.journal 
	where 
		is_active is true
		and date(create_date)<current_date
	group by 1, 2
	) tbl1 
	
	inner join 
		
	(select mobile_no, id account_id, start_balance
	from public.account
	) tbl2 using(mobile_no, account_id); 

do $$

declare
	var_date date:='2019-07-01'; 

begin
	raise notice 'New OP goes below:';

	loop
		delete from data_vajapora.tallykhata_transacting_user_date_sequence 
		where report_date=var_date; 
	
		insert into data_vajapora.tallykhata_transacting_user_date_sequence
		select distinct mobile_no, var_date report_date
		from tallykhata.tallykhata_fact_info_final 
		where 
			entry_type=1
			and created_datetime=var_date; 
		
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 

end $$; 

-- conditions applied
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as

/*#Data Set-1[Data Backup]
 1. Who has never gone to data backup page/screen in their lifetime?
 	- changed to: Who has not gone to data backup page/screen in last 6 months?
 2. Retained from last 6 months & active in last 30 days
 3. 20K data
 4. Only mobile number*/

select mobile_no, 'Set-1[Data Backup]' category
from 
	data_vajapora.help_a tbl1 
	left join 
	data_vajapora.help_b tbl2 using(mobile_no) 
where tbl2.mobile_no is null

union all

/*#Data Set-2[Tagada]
 1. Who never sent a tagada message in lifetime 
 2. Retained from last 6 months & active in last 30 days
 3. Must have credit customers & credit amount as well 
 4. 10K data
 5. Only mobile number*/

select mobile_no, 'Set-2[Tagada]' category
from 
	data_vajapora.help_a tbl1 
	
	inner join
	
	(select distinct mobile_no
	from data_vajapora.baki_from_customers
	where baki>0
	) tbl3 using(mobile_no)
	
	left join 
	
	(select distinct mobile_no
	from public.tagada_log
	) tbl2 using(mobile_no) 
where tbl2.mobile_no is null

union all

/*#Data Set-3[Cash Kena & Expense]
 1. SPU & PU who never use cash kena & Khoroch features in lifetime 
 2. Retained from last 6 months & active in last 30 days
 3. 5K SPU & 5K PU data
 4. Only mobile number*/

select mobile_no, 'Set-3[Cash Kena & Expense] SPU' category
from 
	data_vajapora.help_a tbl2
	left join 
	data_vajapora.help_c tbl3 using(mobile_no)
where 
	tbl3.mobile_no is null
	and tg in('SPU')

union all

select mobile_no, 'Set-3[Cash Kena & Expense] PU' category
from 
	data_vajapora.help_a tbl2
	left join 
	data_vajapora.help_c tbl3 using(mobile_no)
where 
	tbl3.mobile_no is null
	and tg like 'PU%'

union all

/*#Data Set-4[Added Customer]
 1. Minimum 1 customers added but do not record any transaction  
 2. Retained from last 3 months & active in last 30 days
 3. 15K data
 4. Only mobile number*/
 
select mobile_no, 'Set-4[]' category
from 
	data_vajapora.help_f tbl1
	
	inner join 
	
	(select mobile_no, count(id) added_custs
	from public.account
	where type in(2)
	group by 1
	having count(id)>0
	) tbl2 using(mobile_no)
	
	left join 
	
	(select distinct mobile_no 
	from data_vajapora.tallykhata_transacting_user_date_sequence
	) tbl3 using(mobile_no)
where tbl3.mobile_no is null

union all

/*#Data Set-5[NT--]
 1. Active/app open but do not record any transaction in lifetime  
 2. Retained from last 6 months & active in last 30 days
 3. 15K data
 4. Only mobile number*/

select mobile_no, 'Set-5[NT--]' category
from 
	data_vajapora.help_a tbl1
	
	inner join 
	
	(select distinct mobile_no 
	from tallykhata.tallykhata_user_date_sequence_final 
	) tbl2 using(mobile_no)
	
	left join 
	
	(select distinct mobile_no 
	from tallykhata.tallykhata_transacting_user_date_sequence_final 
	) tbl3 using(mobile_no)
where tbl3.mobile_no is null; 

-- for exclusivity
drop table if exists data_vajapora.help_e; 
create table data_vajapora.help_e as
select *, row_number() over(partition by mobile_no order by category desc) seq
from data_vajapora.help_d; 

drop table if exists data_vajapora.event_campaign_5_datasets; 
create table data_vajapora.event_campaign_5_datasets as
select *
from 
	(select mobile_no, category, row_number() over(partition by category) serial
	from data_vajapora.help_e
	where seq=1
	) tbl1 
where 
	((category='Set-1[Data Backup]' and serial<=20000)
	or (category='Set-2[Tagada]' and serial<=10000)
	or (category='Set-3[Cash Kena & Expense] PU' and serial<=5000)
	or (category='Set-3[Cash Kena & Expense] SPU' and serial<=5000)
	or (category='Set-4[]' and serial<=15000)
	or (category='Set-5[NT--]' and serial<=15000)); 

-- view data
select * 
from data_vajapora.event_campaign_5_datasets
order by 2, 3; 

-- see stats
select category, count(mobile_no) merchants
from data_vajapora.event_campaign_5_datasets
group by 1
order by 1; 
