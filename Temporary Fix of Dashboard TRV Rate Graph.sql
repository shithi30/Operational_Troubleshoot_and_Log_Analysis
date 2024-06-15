/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1805379686
- Data: 
- Function: to impact this: tallykhata.tallykhata_user_runrate()
- Table: all dummy tables are generated within data_vajapora
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
I have worked up a temporary solution for dashboard TRV graph using dummy tables. 
No changes are made to main tables/function/dashboard till now. The changes can be incorporated upon a green signal. 
Data, code have been cross-checked by @Minhajul Bhaiya. Demo is available here: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=1805379686 

*/

-- fix for 3RAU
drop table if exists data_vajapora.tallykhata_user_run_rate_dummy;
create table data_vajapora.tallykhata_user_run_rate_dummy as
select *
from tallykhata.tallykhata_user_run_rate
limit 1000; 
truncate table data_vajapora.tallykhata_user_run_rate_dummy;
select *
from data_vajapora.tallykhata_user_run_rate_dummy; 

do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	raise notice 'New OP goes below:'; 
	
	loop
		delete from data_vajapora.tallykhata_user_run_rate_dummy 
		where to_rau_date=var_date; 
	
		insert into data_vajapora.tallykhata_user_run_rate_dummy 
		select 
			s.mobile_no
			,s.rau_category
			,s.to_rau_date
			,count(f.id) as total_activity 
			,sum(case when f.is_suspicious_txn=0 then f.amount else 0 end) as total_amount 
		from 
			(select mobile_no, rau_category, to_rau_date
			from tallykhata.tallykhata_regular_active_user
			where to_rau_date=var_date
			) as s 
			
			inner join 
			
			(select mobile_no, id, amount, is_suspicious_txn
			from tallykhata.tallykhata_user_transaction_info 
			where date(created_datetime)=var_date
			) as f on s.mobile_no = f.mobile_no 
		group by s.mobile_no, s.rau_category, s.to_rau_date; 
	
		raise notice 'Data generated for: %', var_date; 
	
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 
	
end $$; 

select 
	s.to_rau_date
	,'3RAU TTV/user' as category
	,round((sum(s.total_activity)/count(distinct s.mobile_no))::numeric,2) as total_ttv
	,(sum(s.total_amount)/count(distinct s.mobile_no))::int as total_tpv
from data_vajapora.tallykhata_user_run_rate_dummy as s where s.rau_category = 3
group by s.to_rau_date; 

-- fix for PU
drop table if exists data_vajapora.tallykhata_pu_user_run_rate_dummy;
create table data_vajapora.tallykhata_pu_user_run_rate_dummy as
select *
from tallykhata.tallykhata_pu_user_run_rate
limit 1000; 
truncate table data_vajapora.tallykhata_pu_user_run_rate_dummy;
select *
from data_vajapora.tallykhata_pu_user_run_rate_dummy; 

do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	raise notice 'New OP goes below:'; 
	
	loop
		delete from data_vajapora.tallykhata_pu_user_run_rate_dummy 
		where report_date=var_date; 
	
		insert into data_vajapora.tallykhata_pu_user_run_rate_dummy
		select
			s.mobile_no
			,s.report_date
			,count(f.auto_id) as total_activity 
			,sum(f.cleaned_amount) as total_amount 
		from 
			(select mobile_no, report_date 
			from tallykhata.distinct_pu_user
			where report_date=var_date
			) as s 
			inner join 
			(select mobile_no, auto_id, cleaned_amount 
			from tallykhata.tallykhata.tallykhata_fact_info_final
			where created_datetime=var_date
			) as f on s.mobile_no = f.mobile_no 
		group by s.mobile_no, s.report_date;
	
		raise notice 'Data generated for: %', var_date; 
	
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 
	
end $$; 

select 
	s.report_date
	,'PU TTV/user' as category
	,round((sum(s.total_activity)/count(distinct s.mobile_no))::numeric,2) as total_ttv
	,(sum(s.total_amount)/count(distinct s.mobile_no))::int as total_tpv
from data_vajapora.tallykhata_pu_user_run_rate_dummy as s
group by s.report_date;

-- fix for DAU
drop table if exists data_vajapora.daily_active_user_data_dummy;
create table data_vajapora.daily_active_user_data_dummy as
select *
from tallykhata.daily_active_user_data
limit 1000; 
truncate table data_vajapora.daily_active_user_data_dummy;
select *
from data_vajapora.daily_active_user_data_dummy;

do $$ 

declare 
	var_date date:=current_date-30; 
begin 
	raise notice 'New OP goes below:'; 
	
	loop
		delete from data_vajapora.daily_active_user_data_dummy as d where d.report_date=var_date; 
		
		insert into data_vajapora.daily_active_user_data_dummy
		select
			*
		from 
			(
				select 
					ff.created_date as report_date,
					'sync_event' as category,
					count(distinct ff.mobile_no) as total_active_user,
					0 as total_active_user_did_txn,
					0 as total_active_user_did_cs_add,
					0 as nvu_total_active_user_did_txn,
					0 as nvu_total_active_user_did_cs_add,
					0 as total_trt,
					0 as total_cs_add,
					0 as total_ttv,
					0 as total_original_trv,
					0 as total_cleaned_trv,
					0 as total_cleaned_trv_2,
					0 as account_count,
					0 as cash_txn_count, 
					0 as credit_txn_count,
					0 as total_txn_count
				from tallykhata.tallykhata_sync_event_fact_final as ff
				where ff.created_date=var_date
				group by ff.created_date
		
		union all 
			
				select 
					f.created_datetime as report_date,
					'(DAU) Verified' as category,
					count(distinct f.mobile_no) as total_active_user,
					count(distinct case when f.entry_type = 1 then f.mobile_no end) as total_active_user_did_txn,
					count(distinct case when f.entry_type = 2 then f.mobile_no end) as total_active_user_did_cs_add,
					0 as nvu_total_active_user_did_txn,
					0 as nvu_total_active_user_did_cs_add,
					count(case when f.entry_type =1 then f.auto_id end) as total_trt,
					count(case when f.entry_type =2 then f.auto_id end) as total_cs_add,
					count(auto_id) as total_ttv,
					sum(f.input_amount) as total_original_trv,
					sum(f.cleaned_amount) as total_cleaned_trv,
					sum(f.cleaned_amount_2) as total_cleaned_trv_2,
					0 as account_count,
					0 as cash_txn_count, 
					0 as credit_txn_count,
					0 as total_txn_count
				from tallykhata.tallykhata_fact_info_final as f
				where f.created_datetime=var_date
				group by f.created_datetime
			
		union all 
		
				select 
					tbl_1.created_at::date as report_date,
					'Non Verified' as category,
					count(distinct tbl_1.mobile_number) as total_active_user,
					0 as total_active_user_did_txn,
					0 as total_active_user_did_cs_add,
					count(distinct case when tbl_1.account_count >= 1 then tbl_1.mobile_number end) as nvu_total_active_user_did_txn,
					count(distinct case when tbl_1.total_txn_count >= 1 then tbl_1.mobile_number end) as nvu_total_active_user_did_cs_add,
					0 as  total_trt,
					0 as total_cs_add,
					0 as total_ttv,
					0 as total_original_trv,
					0 as total_cleaned_trv,
					0 as total_cleaned_trv_2,
					sum(tbl_1.account_count) as account_count,
					sum(tbl_1.cash_txn_count) as cash_txn_count, 
					sum(tbl_1.credit_txn_count) as credit_txn_count,
					sum(tbl_1.total_txn_count) as total_txn_count
				from (
				
					select 
						ss.mobile_number
						,ss.created_at::date
						,ss.account_count
						,ss.cash_txn_count 
						,ss.credit_txn_count
						,ss.total_txn_count
					from public.user_summary as ss 
					left join public.register_usermobile as i on ss.mobile_number = i.mobile_number
					where i.mobile_number is null 
					and ss.created_at::date=var_date
				) as tbl_1 group by tbl_1.created_at::date
			) as tbl_3
			
			union all 
			
			select 
				tbl_4.report_date,
				'db_plus_event' as category,
				count(distinct tbl_4.mobile_no) as total_active_user,
				0 as total_active_user_did_txn,
				0 as total_active_user_did_cs_add,
				0 as nvu_total_active_user_did_txn,
				0 as nvu_total_active_user_did_cs_add,
				0 as  total_trt,
				0 as total_cs_add,
				0 as total_ttv,
				0 as total_original_trv,
				0 as total_cleaned_trv,
				0 as total_cleaned_trv_2,
				0 as account_count,
				0 as cash_txn_count, 
				0 as credit_txn_count,
				0 as total_txn_count
			from 
				(
					select 
						distinct ff.created_date as report_date,
						ff.mobile_no
					from tallykhata.tallykhata_sync_event_fact_final as ff
					where ff.created_date=var_date
			
				union all 
				
					select 
						distinct f.created_datetime as report_date,
						f.mobile_no
					from tallykhata.tallykhata_fact_info_final as f
					where f.created_datetime=var_date
				) as tbl_4 group by tbl_4.report_date;
		
		raise notice 'Data generated for: %', var_date; 
	
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 
	
end $$; 

select 
	report_date as dau_date,
	'DAU TTV/user' as category,
	round((total_trt/total_active_user_did_txn::float)::numeric,2) as ttv_rate,
	(total_cleaned_trv/total_active_user_did_txn)::int as tpv_rate
from data_vajapora.daily_active_user_data_dummy where category ='(DAU) Verified' and report_date >= current_date-30
order by 1; 

-- unified table 
drop table if exists data_vajapora.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard_dummy;
create table data_vajapora.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard_dummy as
select *
from tallykhata.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard
limit 1000; 
truncate table data_vajapora.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard_dummy;
select *
from data_vajapora.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard_dummy; 

insert into data_vajapora.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard_dummy

-- 3RAU
select 
	s.to_rau_date
	,'3RAU TTV/user' as category
	,round((sum(s.total_activity)/count(distinct s.mobile_no))::numeric,2) as total_ttv
	,(sum(s.total_amount)/count(distinct s.mobile_no))::int as total_tpv
from data_vajapora.tallykhata_user_run_rate_dummy as s where s.rau_category = 3
group by s.to_rau_date

union all 

-- PU
select 
	s.report_date
	,'PU TTV/user' as category
	,round((sum(s.total_activity)/count(distinct s.mobile_no))::numeric,2) as total_ttv
	,(sum(s.total_amount)/count(distinct s.mobile_no))::int as total_tpv
from data_vajapora.tallykhata_pu_user_run_rate_dummy as s
group by s.report_date

union all 

-- DAU
select 
	report_date as dau_date,
	'DAU TTV/user' as category,
	round((total_trt/total_active_user_did_txn::float)::numeric,2) as ttv_rate,
	(total_cleaned_trv/total_active_user_did_txn)::int as tpv_rate
from data_vajapora.daily_active_user_data_dummy where category ='(DAU) Verified' and report_date >= current_date-30; 

-- query on DB
select t1.*
from
(
	select 
	rau_date
	,case when category='3RAU TTV/user' then '3RAU TRV/User'
		  when category='DAU TTV/user' then 'DAU TRV/User'
	      when category='PU TTV/user' then 'PU TRV/User'
	      end as category
	,tpv_rate 
	from data_vajapora.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard_dummy
	where rau_date < current_date -1
) as t1
where t1.category is not null;

-- query for demo
select 
	rau_date,
	sum(case when category='3RAU TTV/user' then tpv_rate else 0 end) tpv_rate_3RAU,
	sum(case when category='DAU TTV/user' then tpv_rate else 0 end) tpv_rate_DAU,
	sum(case when category='PU TTV/user' then tpv_rate else 0 end) tpv_rate_PU
from data_vajapora.cumulative_days_3_and_7_rau_ttv_tpv_rate_dashboard_dummy
where rau_date<current_date-1
group by 1
order by 1; 
