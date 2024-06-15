/*
- Viz: https://docs.google.com/spreadsheets/d/1Yo8227ETNfRoDDWWIh_ve_VZOTsc5bI-q5i4xLQjEQE/edit#gid=842193635
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

-- lifetime TRT
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select mobile_no, count(auto_id) lft_trt
from tallykhata.tallykhata_fact_info_final 
group by 1; 

-- all metrics combined 
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select 
	mobile_no, 
	before_campaign_tg, after_campaign_tg, 
	reg_date, uninstallation_date, 
	if_event_pu, if_trt_pu,
	pu_for_months, 
	last_30_days_trt, if_lft_open, if_lft_txn, lft_trt
from 
	(select 
		mobile_no, 
		before_campaign_tg, after_campaign_tg, 
		reg_date, uninstallation_date, 
		if_event_pu, if_trt_pu,
		last_30_days_trt, if_lft_open, if_lft_txn, lft_trt,
		count(distinct left(report_date::text, 7)) pu_for_months
	from
		-- PU for months
		tallykhata.tk_power_users_10 tbl0
		
		inner join 
	
		(-- transitions from PU to uninstalled
		select *
		from 
			(-- transitions
			select 
				mobile_no, 
				before_campaign_tg, 
				case when after_campaign_tg is null then 'uninstalled' else after_campaign_tg end after_campaign_tg
			from 
				(select mobile_no, tg before_campaign_tg
				from cjm_segmentation.retained_users 
				where report_date='2021-11-10'
				) tbl1 
				
				left join 
				
				(select mobile_no, tg after_campaign_tg
				from cjm_segmentation.retained_users 
				where report_date='2021-11-17'
				) tbl2 using(mobile_no)
			) tbl1 
		where 
			after_campaign_tg='uninstalled' 
			and before_campaign_tg ilike '%pu%'
		) tbl1 using(mobile_no)
		
		inner join 
		
		(-- regitration date
		select mobile_number mobile_no, date(created_at) reg_date 
		from public.register_usermobile 
		) tbl2 using(mobile_no)
		
		left join 
		
		(-- if event PU
		select mobile_no, 1 if_event_pu
		from tallykhata.tk_power_users_10 
		where report_date='2021-11-10'::date
		) tbl3 using(mobile_no)
		
		left join 
	
		(-- if TRT PU
		select mobile_no, 1 if_trt_pu
		from tallykhata.tallykhata_transacting_user_date_sequence_final  
		where created_datetime>='2021-11-10'::date-30 and created_datetime<'2021-11-10'::date 
		group by 1 
		having count(created_datetime)>=1
		) tbl4 using(mobile_no)
		
		left join 
		
		(-- uninstallation times from segment data
		select mobile_no, max(report_date)+1 uninstallation_date
		from cjm_segmentation.retained_users 
		where report_date>='2021-11-10' and report_date<='2021-11-17'
		group by 1
		) tbl6 using(mobile_no)
		
		left join 
		
		(-- last 30 days' TRT
		select mobile_no, count(auto_id) last_30_days_trt
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime>='2021-11-10'::date-30 and created_datetime<'2021-11-10'
		group by 1
		) tbl7 using(mobile_no)
		
		left join 
		
		-- lifetime TRT
		data_vajapora.help_b tbl8 using(mobile_no)
		
		left join 
		
		(-- if opens recorded in lifetime
		select distinct mobile_no, 1 if_lft_open 
		from tallykhata.tallykhata_user_date_sequence_final 
		) tbl9 using(mobile_no)
		
		left join 
		
		(-- if transactions recorded in lifetime
		select distinct mobile_no, 1 if_lft_txn 
		from tallykhata.tallykhata_transacting_user_date_sequence_final 
		) tbl10 using(mobile_no)
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
	) tbl1; 

select *
from data_vajapora.help_a;
