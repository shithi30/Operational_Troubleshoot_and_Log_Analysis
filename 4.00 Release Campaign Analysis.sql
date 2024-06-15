/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1lHufXCN1pyBy4Ss7L9QC_TSTgBcWkPD8VIuTUu3tG_w/edit#gid=1940331858
- Function: 
- Table:
- Instructions: 
- Format: https://docs.google.com/spreadsheets/d/1lHufXCN1pyBy4Ss7L9QC_TSTgBcWkPD8VIuTUu3tG_w/edit#gid=552902290
- File: 
- Path: http://localhost:8888/notebooks/CJM%20for%20Automation/version_04_campaigns.ipynb
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): use the Python script instead, for time-to-time commit facility
*/

-- SQL version (previous)
do $$

declare 
	var_seq int:=1;
	var_max_seq int; 
begin
	-- all campaigns
	drop table if exists data_vajapora.campaign_help_a;
	create table data_vajapora.campaign_help_a as
	select request_id, campaign_id, created_at, start_datetime, end_datetime, row_number() over(order by request_id asc) seq
	from 
		(select distinct request_id 
		from test.campaign_data_v3
		) tbl1
		
		inner join
	
		(select 
	        request_id,
	        max(case when schedule_time is not null then schedule_time else created_at end) start_datetime, 
	        max(case when schedule_time is not null then schedule_time else created_at end+interval '12 hours') end_datetime
	    from public.notification_bulknotificationsendrequest
	    group by 1
	    ) tbl2 using(request_id)
	
	    inner join 
	
		(select title campaign_id, max(id) request_id, max(created_at) created_at
		from public.notification_bulknotificationrequest
		group by 1
		) tbl3 using(request_id);
	
	-- how many campaigns to analyze
	select max(seq) into var_max_seq
	from data_vajapora.campaign_help_a; 
	raise notice 'Max seq: %', var_max_seq; 
		
	-- analyze campaigns iteratively 
	raise notice 'New OP goes below:'; 
	loop
		delete from data_vajapora.version_4_campaigns_analysis
		where campaign_id=(select campaign_id from data_vajapora.campaign_help_a where seq=var_seq);
		
		-- TG
		drop table if exists data_vajapora.campaign_help_b;
		create table data_vajapora.campaign_help_b as
		select mobile_no
		from test.campaign_data_v3
		where request_id=(select request_id from data_vajapora.campaign_help_a where seq=var_seq); 
		
		-- clicked inbox
		drop table if exists data_vajapora.campaign_help_c;
		create table data_vajapora.campaign_help_c as
		select mobile_no
		from tallykhata.tallykhata_sync_event_fact_final
		where 
			event_name='inbox_message_open'
			and 
				event_timestamp>=(select start_datetime from data_vajapora.campaign_help_a where seq=var_seq)
				and 
				event_timestamp<=(select end_datetime from data_vajapora.campaign_help_a where seq=var_seq); 
			
		-- sequenced events of TG who clicked inbox
		drop table if exists data_vajapora.campaign_help_e;
		create table data_vajapora.campaign_help_e as
		select *
		from 
			data_vajapora.campaign_help_c tbl1
			inner join 
			data_vajapora.campaign_help_b tbl2 using(mobile_no)
			inner join 
			(select mobile_no, event_name, event_timestamp, row_number() over(partition by mobile_no order by event_timestamp asc, event_name desc) event_seq
			from tallykhata.tallykhata_sync_event_fact_final
			where 
				event_timestamp>=(select start_datetime from data_vajapora.campaign_help_a where seq=var_seq)
				and 
				event_timestamp<=(select end_datetime from data_vajapora.campaign_help_a where seq=var_seq)
			) tbl3 using(mobile_no); 
				
		-- campaign metrics
		insert into data_vajapora.version_4_campaigns_analysis
		select 
			(select request_id from data_vajapora.campaign_help_a where seq=var_seq) request_id, 
			(select campaign_id from data_vajapora.campaign_help_a where seq=var_seq) campaign_id, 
			*
		from
			(select count(distinct mobile_no) tg_count
			from data_vajapora.campaign_help_b
			) tbl1,
			
			(select count(distinct mobile_no) merchants_clicked_inbox
			from 
				data_vajapora.campaign_help_c tbl1
				inner join 
				data_vajapora.campaign_help_b tbl2 using(mobile_no)
			) tbl3,
			
			(select count(distinct tbl1.mobile_no) opened_app_through_inbox
			from 
				data_vajapora.campaign_help_e tbl1 
				inner join 
				data_vajapora.campaign_help_e tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.event_seq=tbl2.event_seq-1)
			where 
				tbl1.event_name='inbox_message_open'
				and tbl2.event_name='app_opened'
			) tbl6,
						
			(select count(distinct mobile_no) merchants_clicked_inbox_and_recorded_credit_txn_on_day
			from 
				data_vajapora.campaign_help_c tbl1
				inner join 
				data_vajapora.campaign_help_b tbl2 using(mobile_no)
				inner join 
				(select mobile_no 
				from tallykhata.tallykhata_fact_info_final
				where
					created_datetime=(select start_datetime from data_vajapora.campaign_help_a where seq=var_seq)::date
					and txn_type like '%CREDIT%'
				) tbl3 using(mobile_no)
			) tbl4, 
			
			(select count(distinct mobile_no) merchants_clicked_inbox_and_recorded_cash_txn_on_day
			from 
				data_vajapora.campaign_help_c tbl1
				inner join 
				data_vajapora.campaign_help_b tbl2 using(mobile_no)
				inner join 
				(select mobile_no 
				from tallykhata.tallykhata_fact_info_final
				where
					created_datetime=(select start_datetime from data_vajapora.campaign_help_a where seq=var_seq)::date
					and txn_type in('MALIK_NILO', 'MALIK_DILO', 'CASH_PURCHASE', 'EXPENSE', 'CASH_SALE', 'CASH_ADJUSTMENT')
				) tbl3 using(mobile_no)
			) tbl5; 
			
		raise notice 'Data generated for seq: %', var_seq;
		var_seq:=var_seq+1;
		if var_seq=var_max_seq+1 then exit;
		end if; 
	end loop; 
end $$; 

select *
from data_vajapora.version_4_campaigns_analysis; 

-- launch: 182
select
	request_id, 
	campaign_id, 
	campaign_date,
	tg_count, 
	message,
	merchants_clicked_inbox, 
	opened_app_through_inbox, 
	merchants_clicked_inbox_and_recorded_credit_txn_on_day, 
	merchants_clicked_inbox_and_recorded_cash_txn_on_day
from 
	data_vajapora.version_4_campaigns_analysis tbl1 
	inner join 
	(select request_id, message, min(date(start_datetime)) campaign_date
	from data_vajapora.campaign_help_a
	group by 1, 2
	)tbl2 using(request_id)
where campaign_id like 'L21%';

-- regular CJM: 135
select
	request_id, 
	campaign_id, 
	campaign_date,
	tg_count, 
	message,
	merchants_clicked_inbox, 
	opened_app_through_inbox, 
	merchants_clicked_inbox_and_recorded_credit_txn_on_day, 
	merchants_clicked_inbox_and_recorded_cash_txn_on_day
from 
	data_vajapora.version_4_campaigns_analysis tbl1 
	inner join 
	(select request_id, message, min(date(start_datetime)) campaign_date
	from data_vajapora.campaign_help_a
	group by 1, 2
	)tbl2 using(request_id)
where campaign_id like 'C21%';

-- custom messaging: 251
select
	request_id, 
	campaign_id, 
	campaign_date,
	tg_count, 
	message,
	merchants_clicked_inbox, 
	opened_app_through_inbox, 
	merchants_clicked_inbox_and_recorded_credit_txn_on_day, 
	merchants_clicked_inbox_and_recorded_cash_txn_on_day
from 
	data_vajapora.version_4_campaigns_analysis tbl1 
	inner join 
	(select request_id, message, min(date(start_datetime)) campaign_date
	from data_vajapora.campaign_help_a
	group by 1, 2
	)tbl2 using(request_id)
where campaign_id like 'CM21%';

-- greeting message: 39
select
	request_id, 
	campaign_id, 
	campaign_date,
	tg_count, 
	message,
	merchants_clicked_inbox, 
	opened_app_through_inbox, 
	merchants_clicked_inbox_and_recorded_credit_txn_on_day, 
	merchants_clicked_inbox_and_recorded_cash_txn_on_day
from 
	data_vajapora.version_4_campaigns_analysis tbl1 
	inner join 
	(select request_id, message, min(date(start_datetime)) campaign_date
	from data_vajapora.campaign_help_a
	group by 1, 2
	)tbl2 using(request_id)
where campaign_id like 'GM21%';

-- rest: 7 
select
	request_id, 
	campaign_id, 
	campaign_date,
	tg_count, 
	message,
	merchants_clicked_inbox, 
	opened_app_through_inbox, 
	merchants_clicked_inbox_and_recorded_credit_txn_on_day, 
	merchants_clicked_inbox_and_recorded_cash_txn_on_day
from 
	data_vajapora.version_4_campaigns_analysis tbl1 
	inner join 
	(select request_id, message, min(date(start_datetime)) campaign_date
	from data_vajapora.campaign_help_a
	group by 1, 2
	)tbl2 using(request_id)
where not
	(campaign_id like 'GM21%'
	or campaign_id like 'CM21%'
	or campaign_id like 'C21%'
	or campaign_id like 'L21%'
	); 
   
-- all: 614
select
	request_id, 
	campaign_id, 
	campaign_date,
	tg_count, 
	message,
	merchants_clicked_inbox, 
	opened_app_through_inbox, 
	merchants_clicked_inbox_and_recorded_credit_txn_on_day, 
	merchants_clicked_inbox_and_recorded_cash_txn_on_day
from 
	data_vajapora.version_4_campaigns_analysis tbl1 
	inner join 
	(select request_id, message, min(date(start_datetime)) campaign_date
	from data_vajapora.campaign_help_a
	group by 1, 2
	)tbl2 using(request_id);

select 182+135+251+39+7; 

