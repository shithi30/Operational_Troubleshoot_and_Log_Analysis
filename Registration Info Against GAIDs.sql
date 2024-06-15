/*
- Summary: https://docs.google.com/spreadsheets/d/1YW6pobF4vVrSWeQ_a4Ydagvo_mCBY4dO0-UWimheyi4/edit#gid=1452913384
- Data: https://docs.google.com/spreadsheets/d/1YW6pobF4vVrSWeQ_a4Ydagvo_mCBY4dO0-UWimheyi4/edit#gid=812442477
- Function: 
- Table: data_vajapora.shareit_campaign_crosscheck
- File: import to DB.ipynb
- Presentation: 
- Email thread: 
- Notes (if any): Use this to import data: http://localhost:8888/notebooks/Import%20from%20csv%20to%20DB/import%20to%20DB.ipynb
*/

-- GAID that gave 2 mobile_nos: 270819be-e38e-4178-861a-a05bbe6b87e1
select "GAID", count(distinct mobile_no)
from 
	data_vajapora.shareit_campaign_crosscheck tbl1
	
	left join 
	
	(select mobile_no, advertise_id
	from 
		(select mobile_no, max(id) id
		from public.register_tallykhatauser
		group by 1
		) tbl1 
		
		inner join 
		
		(select id, mobile_no, advertise_id 
		from public.register_tallykhatauser
		) tbl2 using(mobile_no, id)
	) tbl2 on(tbl1."GAID"=tbl2.advertise_id)
group by 1
order by 2 desc; 

-- data to sheet
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select tbl1.*, mobile_no
from 
	data_vajapora.shareit_campaign_crosscheck tbl1
	
	left join 
	
	(select mobile_no, advertise_id
	from 
		(select mobile_no, max(id) id
		from public.register_tallykhatauser
		group by 1
		) tbl1 
		
		inner join 
		
		(select id, mobile_no, advertise_id 
		from public.register_tallykhatauser
		) tbl2 using(mobile_no, id)
	) tbl2 on(tbl1."GAID"=tbl2.advertise_id);
select *
from data_vajapora.help_a;

drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select tbl2.*, reg_date, district_name, txns, days_active
from 
	data_vajapora.help_a tbl2

	left join 
	
	(select mobile_number, date(created_at) reg_date 
	from public.register_usermobile 
	) tbl3 on(tbl2.mobile_no=tbl3.mobile_number)
	
	left join 
	
	(select mobile, district_name
	from data_vajapora.tk_users_location_sample_final
	) tbl4 on(tbl2.mobile_no=tbl4.mobile)
	
	left join 
		
	(select mobile_no, count(auto_id) txns
	from 
		(select mobile_no 
		from data_vajapora.help_a
		) tbl1
		
		inner join 
		
		(select mobile_no, auto_id
		from tallykhata.tallykhata_fact_info_final 
		) tbl2 using(mobile_no)
	group by 1
	) tbl5 on(tbl2.mobile_no=tbl5.mobile_no)

	left join 
	
	(select mobile_no, count(distinct event_date) days_active 
	from 
		(select mobile_no 
		from data_vajapora.help_a
		) tbl1
		
		inner join 
		
		(select mobile_no, event_date
		from tallykhata.tallykhata_sync_event_fact_final
		where event_name='app_opened'
		) tbl2 using(mobile_no)
	group by 1
	) tbl6 on(tbl2.mobile_no=tbl6.mobile_no); 
select *
from data_vajapora.help_b; 

-- summary 
select 
	(select count("GAID") from data_vajapora.shareit_campaign_crosscheck) gaids_shared,
	count(case when reg_date is not null then "GAID" else null end) regs_found_against_gaids,
	count(case when reg_date is not null then "GAID" else null end)*1.00/(select count("GAID") from data_vajapora.shareit_campaign_crosscheck) regs_found_against_gaids_pct,      
	count(distinct case when district_name is not null then mobile_no else null end) districts_found_against_users,
	count(distinct case when txns is not null then mobile_no else null end) txns_found_against_users,
	count(distinct case when days_active is not null then mobile_no else null end) active_days_found_against_users
from data_vajapora.help_b; 