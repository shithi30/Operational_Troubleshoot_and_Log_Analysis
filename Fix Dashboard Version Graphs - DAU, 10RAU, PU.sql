/*
- Viz: 
	- Mian page: https://datastudio.google.com/u/0/reporting/28d75b3f-3853-4fe0-8440-279eaa6c0e66/page/FajmB
	- Excluded graphs page: https://datastudio.google.com/u/2/reporting/28d75b3f-3853-4fe0-8440-279eaa6c0e66/page/WdxnB/edit
- Data: 
- Function: tallykhata.tallykhata_monthly_data_dashboard()
- Table: tallykhata.app_version_dau_dashboard, tallykhata.app_version_rau_dashboard, tallykhata.app_version_pu_dashboard
- File: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

/* previous codes */

/*-- TallyKhata App Version wise DAU

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

--App Version 10 RAU

	truncate table tallykhata.app_version_rau_dashboard;
	insert into tallykhata.app_version_rau_dashboard
	select 
		s.rau_date,
		case when i.latest_version_number::int4>88 then left(i.latest_version, 5) else left(i.latest_version, 3) end as app_version,
		count(distinct s.mobile_no) as total_user
	from tallykhata.tallykahta_regular_active_user_new as s
	inner join tallykhata.tk_user_app_version as i on s.mobile_no = i.mobile_no
	where s.rau_category= 10 and s.rau_date >= '2020-07-01'
	group by 1, 2; */

/* changes made */

-- daily current versions of DAUs
do $$

declare 
	var_date date; 
begin
	select max(activity_date)-6 into var_date
	from tallykhata.app_version_dau_dashboard; 

	delete from tallykhata.app_version_dau_dashboard 
	where activity_date>=var_date; 
	
	loop
		insert into tallykhata.app_version_dau_dashboard
		select 
			tbl_1.created_datetime as activity_date,
			case when i.latest_version_number::int4>88 then left(i.latest_version, 5) else left(i.latest_version, 3) end as app_version_name,
			count(tbl_1.mobile_no) as total_mobile
		from tallykhata.tk_user_app_version as i inner join
			(
				select distinct mobile_no, created_date created_datetime
				from tallykhata.tallykhata_sync_event_fact_final
				where created_date=var_date
				
				union
				
				select distinct mobile_no, created_datetime
				from tallykhata.tallykhata_fact_info_final
				where created_datetime=var_date
			) as tbl_1 on i.mobile_no = tbl_1.mobile_no 
		group by 1, 2; 
	
		var_date:=var_date+1; 
		if var_date=current_date then exit; 
		end if; 
	end loop;  
end $$; 

/*
select activity_date, sum(total_mobile) total_dau
from tallykhata.app_version_dau_dashboard 
group by 1 
order by 1;

select * 
from tallykhata.app_version_dau_dashboard;
*/

-- daily current versions of RAUs 
truncate table tallykhata.app_version_rau_dashboard;
insert into tallykhata.app_version_rau_dashboard
select 
	s.report_date::date rau_date,
	case when i.latest_version_number::int4>88 then left(i.latest_version, 5) else left(i.latest_version, 3) end as app_version,
	count(s.mobile_no) as total_user
from tallykhata.regular_active_user_event as s
inner join tallykhata.tk_user_app_version as i on s.mobile_no = i.mobile_no
where s.rau_category= 10 and s.report_date >= '2020-07-01'
group by 1, 2; 

-- daily current versions of PUs
truncate table tallykhata.app_version_pu_dashboard;
insert into tallykhata.app_version_pu_dashboard
select 
	s.report_date::date pu_date,
	case when i.latest_version_number::int4>88 then left(i.latest_version, 5) else left(i.latest_version, 3) end as app_version,
	count(s.mobile_no) as total_user
from 
	(select distinct mobile_no, report_date
	from tallykhata.tk_power_users_10 
	) as s
	inner join 
	tallykhata.tk_user_app_version as i on s.mobile_no = i.mobile_no
where s.report_date >= '2020-07-01'
group by 1, 2; 

