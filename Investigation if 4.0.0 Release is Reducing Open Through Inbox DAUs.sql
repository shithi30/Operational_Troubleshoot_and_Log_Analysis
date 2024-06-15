/*
- Viz: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=1495856804
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

do $$ 

declare 
	var_date date:=current_date-30;
begin 
	raise notice 'New OP goes below:'; 

	-- merchants who have updated to 4.0.1 till today
	drop table if exists data_vajapora.temp_c; 
	create table data_vajapora.temp_c as
	select mobile_no
	from tallykhata.tk_user_app_version
	where latest_version='4.0.1'; 

	loop
		delete from data_vajapora.inbox_open_reduction_analysis
		where report_date=var_date; 
	
		-- merchants updated to 4.0.1 till var_date
		drop table if exists data_vajapora.temp_d; 
		create table data_vajapora.temp_d as
		select mobile_no, date(update_or_reg_datetime) update_date
		from data_vajapora.version_wise_days 
		where 
			app_version_name='4.0.1' 
			and date(update_or_reg_datetime)<=var_date; 
	
		-- sequenced events of DAUs, filtered for necessary events
		drop table if exists data_vajapora.temp_a;
		create table data_vajapora.temp_a as
		select *
		from 
			(select id, mobile_no, event_timestamp, event_name, row_number() over(partition by mobile_no order by event_timestamp asc) seq
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1 
		where event_name in('app_opened', 'inbox_message_open'); 
		
		-- all push-open cases, with first opens of the day identified
		drop table if exists data_vajapora.temp_b;
		create table data_vajapora.temp_b as
		select tbl1.mobile_no, tbl3.id
		from 
			data_vajapora.temp_a tbl1
			inner join 
			data_vajapora.temp_a tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
			left join 
			(select mobile_no, min(id) id 
			from data_vajapora.temp_a 
			where event_name='app_opened'
			group by 1
			) tbl3 on(tbl2.id=tbl3.id)
		where 
			tbl1.event_name='inbox_message_open'
			and tbl2.event_name='app_opened'; 
		
		-- necessary statistics
		insert into data_vajapora.inbox_open_reduction_analysis
		select
			var_date report_date,
			(select count(distinct mobile_no) from data_vajapora.temp_a where event_name='inbox_message_open') inbox_opened_merchants,
			count(distinct case when id is not null then mobile_no else null end) first_open_through_inbox_merchants, 
			count(distinct case when id is not null and tbl2.mobile_no is not null then mobile_no else null end) first_open_through_inbox_merchants_updated,
			count(distinct case when id is not null and tbl2.mobile_no is null then mobile_no else null end) first_open_through_inbox_merchants_not_updated,
			count(distinct case when id is not null and tbl3.mobile_no is not null then mobile_no else null end) first_open_through_inbox_merchants_updated_campaign_day
		from 
			data_vajapora.temp_b tbl1 
			left join 
			data_vajapora.temp_c tbl2 using(mobile_no)
			left join 
			data_vajapora.temp_d tbl3 using(mobile_no); 
		
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date-20 then exit; 
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.inbox_open_reduction_analysis
order by 1; 
