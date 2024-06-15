/*
- Viz: https://datastudio.google.com/u/0/reporting/28d75b3f-3853-4fe0-8440-279eaa6c0e66/page/FajmB
- Data: 
- Function: tallykhata.tallykhata_monthly_data_dashboard()
- Table:
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

/* daily versions of DAUs: discarded afterwards */
-- for viz on db
select 
	s.activity_date::date,
	s.app_version_name,
	sum(s.total_mobile) as total_user,
	ROW_NUMBER () OVER (ORDER BY s.activity_date::date) as row_no
from tallykhata.app_version_dau_dashboard as s
group by s.activity_date,s.app_version_name ;

-- previous code
truncate table tallykhata.app_version_dau_dashboard;
insert into tallykhata.app_version_dau_dashboard
select 
	tbl_1.created_datetime as activity_date,
	case when i.app_version_number::int4 >88 then left(i.app_version_name,5) else left(i.app_version_name,3) end as app_version_name, 
	count(distinct tbl_1.mobile_no) as total_mobile
from tallykhata.tallykhata_user_personal_info as i inner join
	(
		select ii.mobile_no,ii.created_datetime::date from tallykhata.tallykhata_user_transaction_info as ii
		union all 
		select i.mobile_no,i.create_date::date from tallykhata.tallykhata_customer_supplier_info_details as i
	) as tbl_1 on i.mobile = tbl_1.mobile_no where tbl_1.created_datetime>='2020-07-01' and tbl_1.created_datetime < current_date
group by tbl_1.created_datetime,case when i.app_version_number::int4 >88 then left(i.app_version_name,5) else left(i.app_version_name,3) end;

-- revised code
truncate table tallykhata.app_version_dau_dashboard;
insert into tallykhata.app_version_dau_dashboard
select 
	created_datetime activity_date, 
	case when app_version_name>='2.7.1' then left(app_version_name, 5) else left(app_version_name, 3) end as app_version_name, -- change
	count(mobile_no) total_mobile
from 
	(-- last version-date till DAU date
	select tbl2.created_datetime, tbl2.mobile_no, max(update_or_reg_date) highest_update_or_reg_date
	from 
		(-- days of updates/regs
		select mobile_no, date(update_or_reg_datetime) update_or_reg_date
		from data_vajapora.version_wise_days
		) tbl1 
		
		inner join 
		
		(-- txn DAUs on a date
		select distinct mobile_no, created_datetime 
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime>='2020-07-01' and created_datetime<=current_date-1
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.update_or_reg_date<=created_datetime)
	group by 1, 2
	) tbl1
	
	inner join 
	
	(-- corresponding user with version on DAU-date
	select mobile_no, app_version_name, date(update_or_reg_datetime) highest_update_or_reg_date
	from data_vajapora.version_wise_days
	) tbl2 using(mobile_no, highest_update_or_reg_date)
group by 1, 2; 


/* daily versions of RAUs: discarded afterwards */
-- viz on DB
select 
	 rau_date
	,app_version
	,total_user,
	ROW_NUMBER () OVER (ORDER BY rau_date::date) as row_no
from tallykhata.app_version_rau_dashboard; 

-- previous code
truncate table tallykhata.app_version_rau_dashboard;
insert into tallykhata.app_version_rau_dashboard
select 
	s.rau_date,
	case when i.app_version_number::int4 >88 then left(i.app_version_name,5) else left(i.app_version_name,3) end as app_version,
	count(distinct s.mobile_no) as total_user
from tallykhata.tallykahta_regular_active_user_new as s
inner join tallykhata.tallykhata_user_personal_info as i on s.mobile_no = i.mobile 
where s.rau_category= 10 and s.rau_date >= '2020-07-01'
group by s.rau_date,case when i.app_version_number::int4 >88 then left(i.app_version_name,5) else left(i.app_version_name,3) end;

-- new code 
truncate table tallykhata.app_version_rau_dashboard;
insert into tallykhata.app_version_rau_dashboard
select 
	rau_date,
	case when app_version_name>='2.7.1' then left(app_version_name, 5) else left(app_version_name, 3) end as app_version, -- change
	count(mobile_no) total_user
from 
	(-- last version-date till 10RAU date
	select tbl2.rau_date, tbl2.mobile_no, max(update_or_reg_date) highest_update_or_reg_date
	from 
		(-- days of updates/regs
		select mobile_no, date(update_or_reg_datetime) update_or_reg_date
		from data_vajapora.version_wise_days
		) tbl1 
		
		inner join 
		
		(-- 10RAUs on a date
		select mobile_no, rau_date 
		from tallykhata.tallykahta_regular_active_user_new 
		where 
			rau_category=10
			and rau_date>='2020-07-01' and rau_date<=current_date-1
		) tbl2 on(tbl1.mobile_no=tbl2.mobile_no and tbl1.update_or_reg_date<=rau_date)
	group by 1, 2
	) tbl1
	
	inner join 
	
	(-- corresponding user with version on 10RAU-date
	select mobile_no, app_version_name, date(update_or_reg_datetime) highest_update_or_reg_date
	from data_vajapora.version_wise_days
	) tbl2 using(mobile_no, highest_update_or_reg_date)
group by 1, 2; 


/* daily current versions of DAUs */
-- previous code 
truncate table tallykhata.app_version_dau_dashboard;
insert into tallykhata.app_version_dau_dashboard
select 
	tbl_1.created_datetime as activity_date,
	case when i.app_version_number::int4 >88 then left(i.app_version_name,5) else left(i.app_version_name,3) end as app_version_name,
	count(distinct tbl_1.mobile_no) as total_mobile
from tallykhata.tallykhata_user_personal_info as i inner join
	(
		select ii.mobile_no,ii.created_datetime::date from tallykhata.tallykhata_user_transaction_info as ii
		union all 
		select i.mobile_no,i.create_date::date from tallykhata.tallykhata_customer_supplier_info_details as i
	) as tbl_1 on i.mobile = tbl_1.mobile_no where tbl_1.created_datetime>='2020-07-01' and tbl_1.created_datetime < current_date
group by tbl_1.created_datetime,case when i.app_version_number::int4 >88 then left(i.app_version_name,5) else left(i.app_version_name,3) end;

-- revised code
truncate table tallykhata.app_version_dau_dashboard;
insert into tallykhata.app_version_dau_dashboard
select 
	tbl_1.created_datetime as activity_date,
	case when i.latest_version_number::int4>88 then left(i.latest_version, 5) else left(i.latest_version, 3) end as app_version_name,
	count(distinct tbl_1.mobile_no) as total_mobile
from tallykhata.tk_user_app_version as i inner join
	(
		select distinct mobile_no, created_datetime 
		from tallykhata.tallykhata_fact_info_final 
		where created_datetime>='2020-07-01' and created_datetime<=current_date-1
	) as tbl_1 on i.mobile_no = tbl_1.mobile_no 
group by 1, 2; 


/* daily current versions of RAUs */
-- previous code
truncate table tallykhata.app_version_rau_dashboard;
insert into tallykhata.app_version_rau_dashboard
select 
	s.rau_date,
	case when i.app_version_number::int4 >88 then left(i.app_version_name,5) else left(i.app_version_name,3) end as app_version,
	count(distinct s.mobile_no) as total_user
from tallykhata.tallykahta_regular_active_user_new as s
inner join tallykhata.tallykhata_user_personal_info as i on s.mobile_no = i.mobile 
where s.rau_category= 10 and s.rau_date >= '2020-07-01'
group by s.rau_date,case when i.app_version_number::int4 >88 then left(i.app_version_name,5) else left(i.app_version_name,3) end;

-- revised code
truncate table tallykhata.app_version_rau_dashboard;
insert into tallykhata.app_version_rau_dashboard
select 
	s.rau_date,
	case when i.latest_version_number::int4>88 then left(i.latest_version, 5) else left(i.latest_version, 3) end as app_version,
	count(distinct s.mobile_no) as total_user
from tallykhata.tallykahta_regular_active_user_new as s
inner join tallykhata.tk_user_app_version as i on s.mobile_no = i.mobile_no
where s.rau_category= 10 and s.rau_date >= '2020-07-01'
group by 1, 2; 
