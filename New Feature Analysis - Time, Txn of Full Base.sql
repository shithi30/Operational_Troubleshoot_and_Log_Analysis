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
- Email thread: 
- Notes (if any): sheets given above codes
*/

-- for txns
do $$

declare
	var_date date:=current_date-7;
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.cash_usage_anals
		where report_date=var_date; 
	
		-- all cash txns of the day
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select mobile_no, id, txn_type
		from public.journal
		where 
			date(create_date)=var_date
			and txn_type in(1, 2, 6, 7, 8);
		
		-- cash statistics of the day
		insert into data_vajapora.cash_usage_anals
		select 
			var_date report_date,
			count(distinct mobile_no) merchants_used_cash_txn,
			count(id) merchants_total_cash_txn,
			
			count(distinct case when txn_type=6 then mobile_no else null end) merchants_used_malik_nilo,
			count(distinct case when txn_type=7 then mobile_no else null end) merchants_used_malik_dilo,
			count(distinct case when txn_type=8 then mobile_no else null end) merchants_used_cash_adj,
			count(distinct case when txn_type in(6, 7, 8) then mobile_no else null end) merchants_used_nilo_dilo_adj,
			
			count(case when txn_type=6 then id else null end) malik_nilo_trt,
			count(case when txn_type=7 then id else null end) malik_dilo_trt,
			count(case when txn_type=8 then id else null end) cash_adj_trt,
			count(case when txn_type in(6, 7, 8) then id else null end) malik_nilo_dilo_adj_trt
		from data_vajapora.help_a; 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 
end $$; 

-- for time
do $$

declare
	var_date date:=current_date-30;
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.cash_usage_anals_time
		where report_date=var_date; 
	
		-- sequenced events of merchants
		drop table if exists data_vajapora.help_b;
		create table data_vajapora.help_b as
		select *, row_number() over(partition by mobile_no order by event_timestamp asc) seq
		from 
			(select mobile_no, event_name, event_timestamp
			from tallykhata.tallykhata_sync_event_fact_final
			where event_date=var_date
			) tbl1; 
		
		-- daily time spent on cash
		insert into data_vajapora.cash_usage_anals_time
		select var_date report_date, *
		from 
			(-- time spent on cash
			select 
				count(mobile_no) merchants_used_cash_event,
				sum(sec_spent_on_cash) merchants_cash_event_time,
				sum(sec_spent_on_cash)/(count(mobile_no)*1.00) merchants_avg_cash_event_sec,
				sum(sec_spent_on_cash)/(count(mobile_no)*60.00) merchants_avg_cash_event_min
			from 
				(select 
					tbl1.mobile_no,
					sum
					(
						 date_part('hour', tbl2.event_timestamp-tbl1.event_timestamp)*3600
						+date_part('minute', tbl2.event_timestamp-tbl1.event_timestamp)*60
						+date_part('second', tbl2.event_timestamp-tbl1.event_timestamp)
					) sec_spent_on_cash
				from 
					(select mobile_no, event_name, event_timestamp, seq 
					from data_vajapora.help_b 
					where event_name in 
						('cash',
						'cash_becha_entry',
						'cash_becha_entry_screen',
						'cash_box_adjustment',
						'cash_box_entry_list',
						'cash_box_open',
						'cash_box_report_details_view',
						'cash_box_report_view',
						'cash_hishab_view',
						'cash_kena_entry',
						'cash_kena_entry_screen',
						'cash_report_view',
						'khoroch_entry',
						'khoroch_entry_screen',
						'malik_dilo_entry',
						'malik_dilo_entry_screen',
						'malik_nilo_entry',
						'malik_nilo_entry_screen',
						'maliker_report_view'
						)
					) tbl1 
					
					inner join 
					
					data_vajapora.help_b tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1)
				group by 1
				) tbl1 
			) tbl1; 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 
end $$; 

-- for sheet: https://docs.google.com/spreadsheets/d/1QH4tUg1L63RukrGgoCm6wZkWIyvwUb_g8Ldpf7u9kvE/edit#gid=0
select 
	report_date, 
	
	merchants_used_cash_txn, 
	merchants_total_cash_txn, 
	merchants_total_cash_txn*1.00/merchants_used_cash_txn merchants_avg_cash_txn,
	
	merchants_used_cash_event, 
	merchants_cash_event_time, 
	merchants_avg_cash_event_sec, 
	merchants_avg_cash_event_min
from 
	data_vajapora.cash_usage_anals tbl1 
	inner join 
	data_vajapora.cash_usage_anals_time tbl2 using(report_date)
where merchants_used_cash_txn!=0; 

-- for sheet: https://docs.google.com/spreadsheets/d/1QH4tUg1L63RukrGgoCm6wZkWIyvwUb_g8Ldpf7u9kvE/edit#gid=233420157
select 
	report_date, 
	
	merchants_used_cash_txn, 
	merchants_total_cash_txn, 
	
	merchants_used_malik_nilo, 
	merchants_used_malik_dilo, 
	merchants_used_cash_adj, 
	merchants_used_nilo_dilo_adj, 
	
	malik_nilo_trt, 
	malik_dilo_trt, 
	cash_adj_trt, 
	malik_nilo_dilo_adj_trt,
	
	merchants_total_cash_txn*1.00/merchants_used_cash_txn merchants_avg_cash_txn,
	
	merchants_used_cash_event, merchants_cash_event_time, merchants_avg_cash_event_sec, merchants_avg_cash_event_min
from 
	data_vajapora.cash_usage_anals tbl1
	inner join 
	data_vajapora.cash_usage_anals_time tbl2 using(report_date)
where report_date>='2021-09-29'; 

-- for sheet: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=544623018
do $$

declare
	var_date date:=current_date-3;
begin
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.txn_usage
		where report_date=var_date; 
	
		insert into data_vajapora.txn_usage
		select 
			var_date report_date,
			count(case when txn_type in('MALIK_NILO', 'MALIK_DILO', 'CASH_PURCHASE', 'CASH_SALE', 'CASH_ADJUSTMENT') then auto_id else null end) all_cash_txns,
			count(case when txn_type in('MALIK_NILO', 'MALIK_DILO', 'CASH_ADJUSTMENT') then auto_id else null end) new_cash_txns,
			count(case when txn_type in('CREDIT_PURCHASE_RETURN', 'CREDIT_SALE', 'CREDIT_SALE_RETURN', 'CREDIT_PURCHASE') then auto_id else null end) all_tally_txns,
			count(case when txn_type in('Add Customer') then auto_id else null end) add_customer_txns,
			count(case when txn_type in('Add Supplier') then auto_id else null end) add_supplier_txns
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime=var_date; 
	
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 
end $$; 

select *
from data_vajapora.txn_usage; 
