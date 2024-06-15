/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=1012478760
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

do $$ 

declare 
	var_date date:='2021-11-01'::date; 
begin 
	raise notice 'New OP goes below: '; 
	loop
		delete from data_vajapora.spu_inc_analysis 
		where report_date=var_date; 
	
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select mobile_no, auto_id, created_datetime 
		from tallykhata.tallykhata_fact_info_final 
		where 
			entry_type=1
			and created_datetime>var_date-30 and created_datetime<=var_date; 
		
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select
			mobile_no, 
			count(distinct created_datetime) txn_days_last_30_days,
			count(auto_id) txns_last_30_days, 
			count(case when created_datetime>var_date-28 and created_datetime<=var_date-21 then auto_id else null end) last_4_week_txns,
			count(case when created_datetime>var_date-21 and created_datetime<=var_date-14 then auto_id else null end) last_3_week_txns,
			count(case when created_datetime>var_date-14 and created_datetime<=var_date-07 then auto_id else null end) last_2_week_txns,
			count(case when created_datetime>var_date-07 and created_datetime<=var_date-00 then auto_id else null end) last_1_week_txns
		from data_vajapora.help_a
		group by 1; 
		
		insert into data_vajapora.spu_inc_analysis
		select 
			var_date report_date, 
			count(case when 
				1=1
				and txn_days_last_30_days>=24 
				and txns_last_30_days>=60 
				and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10
			then mobile_no else null end) spu, 
			
			count(case when
				1=1
				and txn_days_last_30_days>=24 
				-- and txns_last_30_days>=60 
				-- and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10
			then mobile_no else null end) txn_days_last_30_days_greaterequal_24, 
			
			count(case when 
				1=1
				-- and txn_days_last_30_days>=24 
				and txns_last_30_days>=60 
				-- and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10
			then mobile_no else null end) txns_last_30_days_greaterequal_60, 
			
			count(case when 
				1=1
				-- and txn_days_last_30_days>=24 
				-- and txns_last_30_days>=60 
				and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10
			then mobile_no else null end) last_weeks_txns_greaterequal_10, 
			
			count(case when reg_date<var_date-120 and txn_days_last_30_days>=24 and txns_last_30_days>=60 and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10 then mobile_no else null end) reg_in_more_than_120_days,
			count(case when reg_date>=var_date-120 and reg_date<var_date-105 and txn_days_last_30_days>=24 and txns_last_30_days>=60 and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10 then mobile_no else null end) reg_in_last_105_to_120_days,
			count(case when reg_date>=var_date-105 and reg_date<var_date-90 and txn_days_last_30_days>=24 and txns_last_30_days>=60 and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10 then mobile_no else null end) reg_in_last_90_to_105_days,
			count(case when reg_date>=var_date-90 and reg_date<var_date-75 and txn_days_last_30_days>=24 and txns_last_30_days>=60 and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10 then mobile_no else null end) reg_in_last_75_to_90_days,
			count(case when reg_date>=var_date-75 and reg_date<var_date-60 and txn_days_last_30_days>=24 and txns_last_30_days>=60 and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10 then mobile_no else null end) reg_in_last_60_to_75_days,
			count(case when reg_date>=var_date-60 and reg_date<var_date-45 and txn_days_last_30_days>=24 and txns_last_30_days>=60 and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10 then mobile_no else null end) reg_in_last_45_to_60_days,
			count(case when reg_date>=var_date-45 and reg_date<var_date-30 and txn_days_last_30_days>=24 and txns_last_30_days>=60 and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10 then mobile_no else null end) reg_in_last_30_to_45_days,
			count(case when reg_date>=var_date-30 and reg_date<var_date-00 and txn_days_last_30_days>=24 and txns_last_30_days>=60 and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10 then mobile_no else null end) reg_in_last_30_days,
			count(case when reg_date>=var_date and txn_days_last_30_days>=24 and txns_last_30_days>=60 and last_4_week_txns>=10 and last_3_week_txns>=10 and last_2_week_txns>=10 and last_1_week_txns>=10 then mobile_no else null end) reg_on_or_after_report_date
		from 
			data_vajapora.help_b tbl1 
			
			inner join 
			
			(select mobile_number mobile_no, date(created_at) reg_date 
			from public.register_usermobile
			) tbl2 using(mobile_no); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date='2021-12-01'::date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	report_date,
	spu,
	db_spu,
	txn_days_last_30_days_greaterequal_24,
	txns_last_30_days_greaterequal_60,
	last_weeks_txns_greaterequal_10,
	reg_in_more_than_120_days,
	reg_in_last_105_to_120_days,
	reg_in_last_90_to_105_days,
	reg_in_last_75_to_90_days,
	reg_in_last_60_to_75_days,
	reg_in_last_45_to_60_days,
	reg_in_last_30_to_45_days,
	reg_in_last_30_days, 
	reg_on_or_after_report_date
from 
	data_vajapora.spu_inc_analysis tbl1 
	inner join 
	(select report_date, count(mobile_no) db_spu
	from tallykhata.tk_spu_aspu_data 
	where pu_type='SPU'
	group by 1
	) tbl2 using(report_date)
order by 1; 

