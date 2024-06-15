/*
- Viz: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit#gid=0
- Data: 
- Function: 
- Table:
- Instructions: 3. We see first day TXN % is down. How about first day average time spent by users?
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: Request for first day's transacting users time spent trend analysis!
- Notes (if any): 
	Sir, we have analyzed merchants' registration-day time spending behavior since 01-Nov-21 till date. We have found an overall downtrend in the pattern.  
*/

do $$ 

declare 
	var_date date:='2021-10-31'::date; 
begin 
	raise notice 'New OP goes below:'; 
	loop 
		delete from data_vajapora.reg_day_time_spent 
		where report_date=var_date; 
	
		-- sequenced terminal events on reg date
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select *, row_number() over(partition by mobile_no order by event_timestamp asc) seq
		from 
			(select mobile_no, event_name, event_timestamp
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_name in('app_in_background', 'app_opened', 'app_launched', 'app_closed')
				and event_date=var_date
			) tbl1 
			
			inner join 
			
			(select mobile_number mobile_no
			from public.register_usermobile 
			where date(created_at)=var_date
			) tbl2 using(mobile_no); 
		
		-- avg. mins spent on reg date
		insert into data_vajapora.reg_day_time_spent
		select 
			var_date report_date,
			
			count(tbl1.mobile_no) reg_merchants_recorded_time, 
			sum(min_spent) reg_merchant_total_min_spent, 
			avg(min_spent) reg_merchant_avg_min_spent, 
			
			count(case when tbl2.mobile_no is not null then tbl1.mobile_no else null end) reg_txn_merchants_recorded_time, 
			sum(case when tbl2.mobile_no is not null then min_spent else null end) reg_txn_merchant_total_min_spent,
			avg(case when tbl2.mobile_no is not null then min_spent else null end) reg_txn_merchant_avg_min_spent, 
			
			count(case when tbl2.mobile_no is null then tbl1.mobile_no else null end) reg_nontxn_merchants_recorded_time, 
			sum(case when tbl2.mobile_no is null then min_spent else null end) reg_nontxn_merchant_total_min_spent, 
			avg(case when tbl2.mobile_no is null then min_spent else null end) reg_nontxn_merchant_avg_min_spent
		from 
			(select mobile_no, sum(sec_spent)/60.00 min_spent
			from 
				(select 
					tbl1.mobile_no, 
					date_part('hour', tbl2.event_timestamp-tbl1.event_timestamp)*3600
					+date_part('minute', tbl2.event_timestamp-tbl1.event_timestamp)*60
					+date_part('second', tbl2.event_timestamp-tbl1.event_timestamp)
					sec_spent
				from 
					data_vajapora.help_a tbl1
					inner join 
					data_vajapora.help_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
				where 
					tbl1.event_name in('app_opened', 'app_launched')
					and tbl2.event_name in('app_in_background', 'app_closed')
				) tbl1 
			group by 1
			) tbl1 
			
			left join 
			
			(select distinct mobile_no
			from tallykhata.tallykhata_fact_info_final 
			where created_datetime=var_date
			) tbl2 using(mobile_no); 

		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop;  
end $$; 

select 
	report_date,
	merchants_registered,
	reg_merchants_recorded_time,
	reg_merchant_total_min_spent,
	reg_merchant_avg_min_spent,
	reg_txn_merchants_recorded_time,
	reg_txn_merchant_total_min_spent,
	reg_txn_merchant_avg_min_spent,
	reg_nontxn_merchants_recorded_time,
	reg_nontxn_merchant_total_min_spent,
	reg_nontxn_merchant_avg_min_spent
from 
	data_vajapora.reg_day_time_spent tbl1 
	
	inner join 

	(select date(created_at) report_date, count(mobile_number) merchants_registered 
	from public.register_usermobile
	group by 1
	) tbl2 using(report_date)
order by 1 desc; 