/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=507917107
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: https://docs.google.com/presentation/d/1KeKjbSasRnl1T0oXPj-m1EwHlZRtoT764AVL3d7z4cg/edit#slide=id.ge62f3976a2_0_0
- Email thread: Internet and SMS Usages Stats of TallyKhata
- Notes (if any): 

Daily internet consumption of a user (Avg.)
Monthly internet consumption of a user (Avg.)
Monthly internet consumption of TallyKhata (Avg.)
Daily SMS usages of a user (Avg.)
Monthly SMS usages of a user (Avg.)
Monthly SMS usages of TallyKhata (Avg.)
Transaction and Tagada Separately 
*/

-- all event data of 18 Jul, 21
drop table if exists data_vajapora.help_a;
create table data_vajapora.help_a as
select id, "level", event_name, user_id, created_at, details
from public.eventapp_event 
where date(created_at)='2021-07-18'; 

-- sync event data of 18 Jul, 21
drop table if exists data_vajapora.help_b;
create table data_vajapora.help_b as
select *, (length(details)+length(details)*0.05)*1.00/1024 kbs
from data_vajapora.help_a
where 
	event_name like '%device_to_server_sync%'
	and level='INFO'; 

-- KB consumption data of 18 Jul, 21; for different user-groups
select 
	'2021-07-18'::date date,
	avg(case when dau_mobile_no is not null then kbs_consumed else null end) avg_dau_kbs_consumption, 
	avg(case when pu_mobile_no is not null then kbs_consumed else null end) avg_pu_kbs_consumption, 
	avg(case when rau3_mobile_no is not null then kbs_consumed else null end) avg_rau3_kbs_consumption
from 
	(select user_id mobile_no, sum(kbs) kbs_consumed
	from data_vajapora.help_b
	group by 1
	) tbl1

	left join 
				
	(select distinct mobile_no pu_mobile_no 
	from tallykhata.tk_power_users_10 
	where report_date='2021-07-18'
	) tbl2 on(tbl1.mobile_no=tbl2.pu_mobile_no)
	
	left join 
	
	(select mobile_no dau_mobile_no 
	from tallykhata.tallykhata_user_date_sequence_final 
	where event_date='2021-07-18'
	) tbl3 on(tbl1.mobile_no=tbl3.dau_mobile_no)
	
	left join 
	
	(select mobile_no rau3_mobile_no
	from tallykhata.tallykhata_regular_active_user 
	where 
		rau_category=3
		and rau_date='2021-07-18'
	) tbl4 on(tbl1.mobile_no=tbl4.rau3_mobile_no); 

/*
-- updates by Md. Nazrul Islam 

drop table if exists test.tk_kb_size_analysis;
create table test.tk_kb_size_analysis as 
select * from public.eventapp_event as e 
where e.created_at::date = '2021-07-18'
--and (e.event_name like '%device_to_server%' 
--	or e.event_name like '%sync_app_event%' 
--	or e.event_name like '%sync_location_v3%'
--	)
and e.message like '%request received%';

select * from test.tk_kb_size_analysis;

drop table if exists test.tk_kb_size_analysis_v1;
create table test.tk_kb_size_analysis_v1 as 
select *,(length(details)+length(details)*0.02)*1.00/1024 kbs from test.tk_kb_size_analysis ;

drop table if exists test.tk_kb_size_analysis_v2;
create table test.tk_kb_size_analysis_v2 as
select 
	s.user_id,
	sum(s.kbs) as total_kbs
from test.tk_kb_size_analysis_v1 as s
group by s.user_id;

select * from test.tk_kb_size_analysis_v1;

select * from test.tk_kb_size_analysis_v2;
select avg(total_kbs) from test.tk_kb_size_analysis_v2;

--> Avg. Daily 52KB
*/
	
