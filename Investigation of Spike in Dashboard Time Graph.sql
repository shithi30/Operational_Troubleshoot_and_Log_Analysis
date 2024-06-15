/*
- Viz: 
	- https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=1927201499
	- https://docs.google.com/spreadsheets/d/1PRpEOx6y_u93Hf5qyIG8A1C0X1HC2JJ_frqd3rTaxrg/edit#gid=799702045
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

-- all events
do $$ 

declare 
	var_date date:='04-Nov-22'::date; 
begin 
	raise notice 'New OP goes below:'; 
	
	loop 
		delete from data_vajapora.time_analysis_after_release 
		where date(event_timestamp)=var_date; 
	
		-- relevant events, back-to-back
		drop table if exists data_vajapora.help_a; 
		create table data_vajapora.help_a as
		select
			*, 
			lead(event_name, 1) over(partition by mobile_no order by event_timestamp, id) next_event, 
			lead(event_timestamp, 1) over(partition by mobile_no order by event_timestamp, id) next_event_timestamp
		from 
			(select mobile_no, id, event_timestamp, event_name
			from tallykhata.tallykhata_sync_event_fact_final 
			where 
				event_date=var_date
				and event_name not in('inbox_message_received', 'in_app_message_received')
			) tbl1
			
			inner join 
			
			(select mobile_no 
			from tallykhata.tk_spu_aspu_data 
			where 
				pu_type in('SPU', 'Sticky SPU')
				and report_date=var_date
			) tbl2 using(mobile_no); 
		
		-- time per event
		insert into data_vajapora.time_analysis_after_release
		select 
			*, 
			 date_part('hour', next_event_timestamp-event_timestamp)*3600
			+date_part('minute', next_event_timestamp-event_timestamp)*60 
			+date_part('second', next_event_timestamp-event_timestamp)
			time_spent
		from data_vajapora.help_a 
		where event_name not in('app_closed', 'app_in_background'); 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select * 
from data_vajapora.time_analysis_after_release 
limit 1000;

select 
	date(event_timestamp) report_date, 
	event_name, 
	avg(time_spent) avg_time_spent 
from data_vajapora.time_analysis_after_release 
group by 1, 2
order by 1 asc, 3 desc; 

select 
	date(event_timestamp) report_date, 
	mobile_no, 
	sum(time_spent) time_spent 
from data_vajapora.time_analysis_after_release 
group by 1, 2; 

-- segmented events 
do $$ 

declare 
	var_date date:='04-Nov-22'::date; 
begin 
	raise notice 'New OP goes below:'; 
	
	loop 
		delete from data_vajapora.time_analysis_after_release_2
		where report_date=var_date; 
		
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select
			date(tbl_1.event_timestamp) as report_date,
			tbl_1.mobile_no,
			round((sum(case when tbl_1.event_name in ('cash', 'cash_becha_entry', 'cash_becha_entry_screen', 'cash_box_adjustment', 'cash_box_adjustment_closed', 'cash_box_adjustment_confirm_failed', 'cash_box_adjustment_confirm_success', 'cash_box_entry_list', 'cash_box_open', 'cash_box_report_details_view', 'cash_box_report_view', 'cash_hishab_view', 'cash_kena_entry', 'cash_kena_entry_screen', 'cash_report_view', 'khoroch_entry', 'khoroch_entry_screen', 'malik_dilo_entry', 'malik_dilo_entry_screen', 'malik_nilo_entry', 'malik_nilo_entry_screen', 'maliker_report_view', 'view_cash_box_details', 'daily_summary_open_from_cash_box') then diff_in_sec else 0 end))::numeric ,2) as cashbox_time_spent_in_min,
			round((sum(case when tbl_1.event_name in ('tally', 'report_download_customer_detail', 'report_download_customer_list', 'tagada_message_from_customer_report', 'tagada_message_from_home', 'tagada_empty_confirm_dialog', 'tagada_share', 'add_contact_from_phone_book', 'add_customer') then diff_in_sec else 0 end))::numeric,2) as tally_time_spent_in_min,
			round((sum(case when tbl_1.event_name in ('inbox_message_action', 'inbox_message_open') then diff_in_sec else 0 end))::numeric,2) as inbox_time_spent_in_min,
			round((sum(case when tbl_1.event_name in ('help', 'in_app_message_close', 'in_app_message_link_tap', 'in_app_message_open', 'copy_refer_link', 'manual_data_backup', 'manual_data_backup_from_menu', 'pin_setting', 'profile_edit', 'profile_view', 'refer', 'refer_button_pressed', 'submit_otp', 'verification_screen_for_unverified_users') then diff_in_sec else 0 end))::numeric,2) as other_time_spent_in_min     -- excluding app open, app launch, app in background and app close
		from 
			(
				select 
					id ,
					mobile_no ,
					event_name ,
					event_timestamp,
					lead(event_timestamp) over(partition by mobile_no order by id,event_timestamp) as next_val,
					abs(EXTRACT(EPOCH from(lead(event_timestamp) over(partition by mobile_no order by id,event_timestamp) -event_timestamp))) as diff_in_sec
				from 
					(select mobile_no, id, event_timestamp, event_name
					from tallykhata.tallykhata_sync_event_fact_final 
					where 
						event_date=var_date
						-- and event_name not in('inbox_message_received', 'in_app_message_received')
					) tbl1
					
					inner join 
					
					(select mobile_no 
					from tallykhata.tk_spu_aspu_data 
					where 
						pu_type in('SPU', 'Sticky SPU')
						and report_date=var_date
					) tbl2 using(mobile_no)
			) tbl_1
		where diff_in_sec<3600 
		group by date(tbl_1.event_timestamp),tbl_1.mobile_no
		;
	
		insert into data_vajapora.time_analysis_after_release_2
		select * 
		from data_vajapora.help_b; 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	report_date, 
	mobile_no, 
	cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min time_spent_mahmud
from data_vajapora.time_analysis_after_release_2; 

-- segmented events (excl. rec.)
do $$ 

declare 
	var_date date:='04-Nov-22'::date; 
begin 
	raise notice 'New OP goes below:'; 
	
	loop 
		delete from data_vajapora.time_analysis_after_release_3
		where report_date=var_date; 
		
		drop table if exists data_vajapora.help_b; 
		create table data_vajapora.help_b as
		select
			date(tbl_1.event_timestamp) as report_date,
			tbl_1.mobile_no,
			sum(case when event_name not in('app_closed', 'app_in_background') then diff_in_sec else 0 end) time_in_sec_combined, 
			round((sum(case when tbl_1.event_name in ('cash', 'cash_becha_entry', 'cash_becha_entry_screen', 'cash_box_adjustment', 'cash_box_adjustment_closed', 'cash_box_adjustment_confirm_failed', 'cash_box_adjustment_confirm_success', 'cash_box_entry_list', 'cash_box_open', 'cash_box_report_details_view', 'cash_box_report_view', 'cash_hishab_view', 'cash_kena_entry', 'cash_kena_entry_screen', 'cash_report_view', 'khoroch_entry', 'khoroch_entry_screen', 'malik_dilo_entry', 'malik_dilo_entry_screen', 'malik_nilo_entry', 'malik_nilo_entry_screen', 'maliker_report_view', 'view_cash_box_details', 'daily_summary_open_from_cash_box') then diff_in_sec else 0 end))::numeric ,2) as cashbox_time_spent_in_min,
			round((sum(case when tbl_1.event_name in ('tally', 'report_download_customer_detail', 'report_download_customer_list', 'tagada_message_from_customer_report', 'tagada_message_from_home', 'tagada_empty_confirm_dialog', 'tagada_share', 'add_contact_from_phone_book', 'add_customer') then diff_in_sec else 0 end))::numeric,2) as tally_time_spent_in_min,
			round((sum(case when tbl_1.event_name in ('inbox_message_action', 'inbox_message_open') then diff_in_sec else 0 end))::numeric,2) as inbox_time_spent_in_min,
			round((sum(case when tbl_1.event_name in ('help', 'in_app_message_close', 'in_app_message_link_tap', 'in_app_message_open', 'copy_refer_link', 'manual_data_backup', 'manual_data_backup_from_menu', 'pin_setting', 'profile_edit', 'profile_view', 'refer', 'refer_button_pressed', 'submit_otp', 'verification_screen_for_unverified_users') then diff_in_sec else 0 end))::numeric,2) as other_time_spent_in_min     -- excluding app open, app launch, app in background and app close
		from 
			(
				select 
					id ,
					mobile_no ,
					event_name ,
					event_timestamp,
					lead(event_timestamp) over(partition by mobile_no order by event_timestamp, id) as next_val,
					abs(EXTRACT(EPOCH from(lead(event_timestamp) over(partition by mobile_no order by event_timestamp, id) -event_timestamp))) as diff_in_sec
				from 
					(select mobile_no, id, event_timestamp, event_name
					from tallykhata.tallykhata_sync_event_fact_final 
					where 
						event_date=var_date
						and event_name not in('inbox_message_received', 'in_app_message_received')
					) tbl1
					
					inner join 
					
					(select mobile_no 
					from tallykhata.tk_spu_aspu_data 
					where 
						pu_type in('SPU', 'Sticky SPU')
						and report_date=var_date
					) tbl2 using(mobile_no)
			) tbl_1
		where diff_in_sec<3600 
		group by date(tbl_1.event_timestamp),tbl_1.mobile_no
		;
	
		insert into data_vajapora.time_analysis_after_release_3
		select * 
		from data_vajapora.help_b; 
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop; 
end $$; 

select 
	report_date, 
	mobile_no, 
	cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min time_spent_mahmud_excl_rec
from data_vajapora.time_analysis_after_release_3; 

-- comparison: user by user
select * 
from 
	(select 
		report_date, 
		mobile_no, 
		cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min time_spent_mahmud
	from data_vajapora.time_analysis_after_release_2
	) tbl1 
	
	inner join 
	
	(select 
		date(event_timestamp) report_date, 
		mobile_no, 
		sum(time_spent) time_spent_shithi
	from data_vajapora.time_analysis_after_release 
	group by 1, 2
	) tbl2 using(report_date, mobile_no); 

-- comparison: total_time
select * 
from 
	(select 
		report_date, 
		sum(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min) time_spent_mahmud
	from data_vajapora.time_analysis_after_release_2
	group by 1
	) tbl1 
	
	inner join 
	
	(select 
		date(event_timestamp) report_date, 
		sum(time_spent) time_spent_shithi
	from data_vajapora.time_analysis_after_release 
	group by 1
	) tbl2 using(report_date)

	inner join 
	
	(select 
		report_date, 
		sum(cashbox_time_spent_in_min+tally_time_spent_in_min+inbox_time_spent_in_min+other_time_spent_in_min) time_spent_mahmud_excl_rec, 
		sum(time_in_sec_combined) time_spent_mahmud_excl_rec_combined
	from data_vajapora.time_analysis_after_release_3
	group by 1
	) tbl3 using(report_date); 

-- if duplicates
select report_date, count(mobile_no), count(distinct mobile_no)
from test.userwise_time_spent
where report_date>='06-Nov-22'
group by 1 
order by 1;