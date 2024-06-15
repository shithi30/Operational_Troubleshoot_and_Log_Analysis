/*
- Viz: https://docs.google.com/spreadsheets/d/1pv4DmSqE_noX2674BfittCQrXy7Qjjzg6dNafzg4ly4/edit#gid=0
- Data: https://docs.google.com/spreadsheets/d/1cfP1Y4fuSiCymqrwsmlWRqYz6jGhZSgH1mpPvF1XO5g/edit?pli=1#gid=1311470403
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	We have analysed testing merchants' behavior based on August-2022 data. 
	Testing Merchants : Users who registered, recorded some txn and then called d2s - all on the same day, but didn't record any txn afterward. 
	Findings: 
	- 8 to 10 merchants are found daily who seem to test the app. 
	- The portion of such users is negligible wrt. regular registrations. 

	 On an avg. 25 user tried S2D on the same day after registration, 7 out of 25 seems leaving our platform everyday.
*/

do $$ 

declare 
	var_date date:='2022-08-01'::date; 
begin 
	raise notice 'New OP goes below:'; 

	/*
	-- ~ 1 hr
	drop table if exists data_vajapora.help_a; 
	create table data_vajapora.help_a as
	select user_id mobile_no, created_at s2d_time
	from public.eventapp_event
	where 
		level='INFO'
		and message like 'response%'
		and event_name like 'server_to_device%'
		and date(created_at)>='2022-08-01' and date(created_at)<current_date;
	*/

	loop
		delete from data_vajapora.testing_users_stats_1 
		where report_date=var_date; 
	
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select mobile_number mobile_no, created_at reg_time
		from public.register_usermobile 
		where date(created_at)=var_date; 
	
		insert into data_vajapora.testing_users_stats_1
		select 
			var_date report_date, 
			count(tbl1.mobile_no) merchants_registered, 
			count(case when min_txn_time>reg_time and min_txn_time<last_s2d_time then tbl1.mobile_no else null end) merchants_txned_before_last_s2d, 
			count(case when min_txn_time>reg_time and min_txn_time<last_s2d_time and max_txn_time<last_s2d_time then tbl1.mobile_no else null end) merchants_didnt_txn_after_last_s2d 
		from 
			data_vajapora.help_b tbl1 
			
			left join 
			
			(select mobile_no, max(s2d_time) last_s2d_time
			from data_vajapora.help_a 
			group by 1
			having date(max(s2d_time))=var_date
			) tbl2 using(mobile_no)
			
			left join 
			
			(select mobile_no, min(created_timestamp) min_txn_time, max(created_timestamp) max_txn_time 
			from tallykhata.tallykhata_fact_info_final
			where mobile_no in(select mobile_no from data_vajapora.help_b)
			group by 1
			) tbl3 using(mobile_no); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
	
end $$; 

select * 
from data_vajapora.testing_users_stats_1; 

/*
-- inv. 

drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select mobile_number mobile_no, created_at reg_time
from public.register_usermobile 
where date(created_at)='25-Aug-22'; 
	
select *
from 
	data_vajapora.help_b tbl1 
	
	left join 
	
	(select mobile_no, max(s2d_time) last_s2d_time
	from data_vajapora.help_a 
	group by 1
	having date(max(s2d_time))='25-Aug-22'
	) tbl2 using(mobile_no)
	
	left join 
	
	(select mobile_no, min(created_timestamp) min_txn_time, max(created_timestamp) max_txn_time 
	from tallykhata.tallykhata_fact_info_final
	where mobile_no in(select mobile_no from data_vajapora.help_b)
	group by 1
	) tbl3 using(mobile_no)
where
	min_txn_time>reg_time and min_txn_time<last_s2d_time 
	and max_txn_time<last_s2d_time; 

select * 
from tallykhata.tallykhata_fact_info_final 
where mobile_no='01709191231'; 
*/
