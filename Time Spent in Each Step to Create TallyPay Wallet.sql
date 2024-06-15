/*
- Viz: 
- Data: https://docs.google.com/spreadsheets/d/1g2Hm6qmYJjB870MluhkS1y2-kDYfIGNLTOiCfb-cPdY/edit#gid=1981190893
- Function: 
- Table:
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	There are 1K userbase who registered in TK & TP + NID verified + Face verified + MFS/Bank account verified.
	
	Want to know following details of these wallet's :
	----------------------------------------------------------
	1. How many registered in TK before version 5.0
	2. How many newly registered in TK version >= 5.0
	3. User wise how much time they spent to open wallet [ for each step ]
	4. User wise Average D2S & S2D time
	5. Which cohort is highly adoptive in payment system [ updated or newly registered ]
	6. TG cohort distribution [ 3RAU, SU, PU, churn etc. ]
	7.  Payment type wise usages
*/

-- wallet openers
/*
-- from NP DWH
select distinct p.wallet_no mobile_no
from ods_tp.backend_db__profile p 
left join ods_tp.backend_db__document as d on p.user_id  = d.user_id 
left join ods_tp.backend_db__bank_account  as a on p.user_id  = a.user_id
left join ods_tp.backend_db__mfs_account as m on p.user_id  = m.user_id
where 1=1
and upper(d.doc_type) ='NID'
and p.created_at::date>='2022-09-21'
and p.bank_account_status = 'VERIFIED';
*/ 
select * 
from data_vajapora.wallet_open; 

-- 1. How many registered in TK before version 5.0
-- 2. How many newly registered in TK version >= 5.0
-- if updated or new
drop table if exists data_vajapora.help_a; 
create table data_vajapora.help_a as
select mobile_no, app_version_number, app_version_name, update_date, reg_date
from 
	data_vajapora.wallet_open tbl0 
	
	left join 

	(select mobile mobile_no, max(id) id 
	from public.registered_users
	group by 1 
	) tbl1 using(mobile_no)
	
	left join 
	
	(select id, app_version_name, app_version_number, date(updated_at) update_date
	from public.registered_users
	where app_version_number>105
	) tbl2 using(id) 
	
	left join 
	
	(select mobile_number mobile_no, date(created_at) reg_date 
	from public.register_usermobile 
	) tbl3 using(mobile_no); 

select 
	count(mobile_no) wallet_opened, 
	count(case when update_date>reg_date then mobile_no else null end) wallet_opened_after_update, 
	count(case when update_date=reg_date then mobile_no else null end) wallet_opened_at_reg,
	count(case when update_date is null or reg_date is null then mobile_no else null end) reg_or_update_null
from data_vajapora.help_a; 

-- 4. User wise Average D2S & S2D time
-- wallet openers' last 24 hours' avg. API times
drop table if exists data_vajapora.help_b; 
create table data_vajapora.help_b as
select id
from tallykhata.eventapp_event_temp 
where user_id in(select * from data_vajapora.wallet_open); 

select 
	user_id, 
	avg(case when event_name like 'device_to_server%' and message like '%response%' then total_time end) avg_d2s_time, 
	avg(case when event_name like 'server_to_device%' and message like '%response%' then total_time end) avg_s2d_time
from 
	(select id, total_time, event_name, message, user_id 
	from public.eventapp_event 
	where app_version::int>105
	) tbl1
	inner join 
	data_vajapora.help_b tbl2 using(id) 
group by 1; 

-- 5. Which cohort is highly adoptive in payment system [ updated or newly registered ]
select wallet_type, sum(total_transaction) txn_activities
from 
	(select wallet_no mobile_no, total_transaction
	from data_vajapora.t_marketing_report
	) tbl1 
	
	left join 

	(select 
		mobile_no, 
		case 
			when update_date>reg_date then 'wallet opened after update'
			when update_date=reg_date then 'wallet opened at reg' 
			else 'reg or update date unavailable' 
		end wallet_type 
	from data_vajapora.help_a
	) tbl2 using(mobile_no)
group by 1 
order by 2 desc; 

-- 6. TG cohort distribution [ 3RAU, SU, PU, churn etc. ]
select 
	case when segment is null then 'uninstalled' else segment end segment, 
	count(mobile_no) wallet_users
from 
	data_vajapora.wallet_open tbl1 
	
	left join 
	
	(select 
		mobile_no, 
		max(
			case 
				when tg like '3RAU%' then '3RAU'
				when tg like 'LTU%' then 'LTU'
				when tg like 'PU%' then 'PU'
				when tg like 'Z%' then 'Zombie' 
				when tg in('NT--') then 'NT'
				when tg in('NB0','NN1','NN2-6') then 'NN'
				when tg in('PSU') then 'PSU'
				when tg in('SPU') then 'SU'
				else null
			end
		) segment
	from cjm_segmentation.retained_users 
	where report_date=current_date-1
	group by 1
	) tbl2 using(mobile_no) 
group by 1; 

-- 7.  Payment type wise usages
select txn_type, sum(total_transaction) transactions 
from data_vajapora.t_marketing_report
group by 1;

-- 3. User wise how much time they spent to open wallet [ for each step ]

/*
APIs called:
wallet-lookup-api
/api/v1/user/doc/nid-front
/api/v1/user/doc/nid-back
/api/v1/user/doc/confirm-nid-info
/api/v1/user/doc/face-image
*/

-- first event for wallet open 
drop table if exists data_vajapora.help_c; 
create table data_vajapora.help_c as
select created_at-interval '6 hours' created_at, event_name api, user_id mobile_no
from public.eventapp_event 
where 
	user_id in(select * from data_vajapora.wallet_open)
	and event_name='wallet-lookup-api'
	and message='request received'; 

-- rest of the events for wallet open 
/*
-- from NP DWH
select created_at, url api, user_name mobile_no
from ods_tp.nobopay_api_gw__activity_log
where url in 
	('/api/v1/user/doc/nid-front',
	'/api/v1/user/doc/nid-back',
	'/api/v1/user/doc/confirm-nid-info',
	'/api/v1/user/doc/face-image'
	); 
*/
select *
from data_vajapora.wallet_open_apis; 

-- all events combined
drop table if exists data_vajapora.help_d; 
create table data_vajapora.help_d as
select * from data_vajapora.help_c 
union all
select * from data_vajapora.wallet_open_apis where mobile_no!=''; 

-- events sorted
drop table if exists data_vajapora.help_e; 
create table data_vajapora.help_e as
select *, row_number() over(partition by mobile_no order by created_at asc) seq
from 
	(select mobile_no, api, max(created_at) created_at
	from 
		data_vajapora.help_d tbl1 
		
		inner join 
		
		(select mobile_no, min(created_at) success_datetime
		from data_vajapora.help_d 
		where api='/api/v1/user/doc/face-image'
		group by 1 
		) tbl2 using(mobile_no)
	where 
		created_at<=success_datetime
		and date(created_at)=date(success_datetime)
	group by 1, 2
	) tbl1; 

-- time (each step)
drop table if exists data_vajapora.help_f; 
create table data_vajapora.help_f as
select mobile_no, round((sum(seconds_spent)/60.00)::numeric, 2) mins_to_create_wallet
from 
	(select 
		tbl2.mobile_no, 
		tbl2.api,
		 date_part('hour', tbl2.created_at-tbl1.created_at)*3600
		+date_part('minute', tbl2.created_at-tbl1.created_at)*60
		+date_part('second', tbl2.created_at-tbl1.created_at)
		seconds_spent
	from 
		data_vajapora.help_e tbl1 
		inner join 
		data_vajapora.help_e tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.seq=tbl2.seq-1) 
		inner join 
		(select mobile_no
		from data_vajapora.help_e
		where 
			seq=1 
			and api='wallet-lookup-api'
		) tbl3 on(tbl2.mobile_no=tbl3.mobile_no)
		inner join 
		data_vajapora.wallet_open tbl4 on(tbl3.mobile_no=tbl4.mobile_no)
	) tbl1
group by 1
order by 2 desc; 

-- inv.
select * 
from data_vajapora.help_e
where mobile_no in
	('01821234631',
	'01757711577',
	'01815865700',
	'01816369050',
	'01742407294'); 

-- distribution
select 
	mins_to_create_wallet_cat, 
	count(*) wallets_created 
from 
	(select 
		*, 
		case 
			when mins_to_create_wallet<1 then 'less than 1 min'
			when mins_to_create_wallet>=1 and mins_to_create_wallet<2 then '1 to 2 mins'
			when mins_to_create_wallet>=2 and mins_to_create_wallet<3 then '2 to 3 mins'
			when mins_to_create_wallet>=3 and mins_to_create_wallet<4 then '3 to 4 mins'
			when mins_to_create_wallet>=4 and mins_to_create_wallet<=5 then '4 to 5 mins'
			else 'more than 5 mins'
		end mins_to_create_wallet_cat
	from data_vajapora.help_f
	) tbl1 
group by 1
order by 2 desc;
