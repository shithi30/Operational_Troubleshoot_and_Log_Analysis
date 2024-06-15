/*
- Viz: 
- Data: 
- Function: tallykhata.tallykhata_dau_app_open_distribution()
- Table: tallykhata.app_open_dau_distribution_final
- Instructions: 
- Format: 
- File: 
- Path: 
- Document/Presentation/Dashboard: https://datastudio.google.com/u/0/reporting/28d75b3f-3853-4fe0-8440-279eaa6c0e66/page/UgzPC
- Email thread: 
- Notes (if any): 
	Sum of using DAU's cohort trend are not matched with reported DAU & Google DAU! 
	Even some days it showing higher than Google DAU! Is there any issue? @Samir bhai 
	
	Changing date(created_date) to event_date fixed it. 
*/

-- regenerating data
do $$

declare
	var_date date:='2022-02-01'; 

begin
	raise notice 'Programme execution started...%s',now();

	loop
		drop table if exists test.naz_test_app_open_dau_distribution;
		create table test.naz_test_app_open_dau_distribution as 
		select 
			f.mobile_no,
			f.created_datetime,
			'otp_verified' as user_type
		from tallykhata.tallykhata.tallykhata_fact_info_final as f
		where f.created_datetime::date = var_date
		
		union 
	
		select 
			ff.mobile_no,
			ff.event_date,
			'otp_verified' as user_type
		from tallykhata.tallykhata.tallykhata_sync_event_fact_final as ff
		where ff.event_name not in ('in_app_message_received','inbox_message_received')
		-- and ff.created_date::date = var_date
		and ff.event_date = var_date -- change here
		
		union 
		
		select 
			ss.mobile_number,
			ss.created_at::date as create_date,
			'unverified' as user_type
		from public.user_summary as ss 
		left join public.register_usermobile as i on ss.mobile_number = i.mobile_number
		where i.mobile_number is null
		and ss.created_at::date = var_date;
		
		-- raise notice ' inserted into test.naz_test_app_open_dau_distribution for: %',var_date;
	
		drop table if exists test.naz_test_app_open_dau_distribution_v1;
		create table test.naz_test_app_open_dau_distribution_v1 as 
		select 
			d.*,
			m.created_at::date as reg_date
		from test.naz_test_app_open_dau_distribution as d 
		inner join public.register_usermobile as m on d.mobile_no = m.mobile_number
		where d.user_type = 'otp_verified' and created_datetime =var_date
		
		union all 
		
		select 
			d.*,
			m.created_at::date as reg_date
		from test.naz_test_app_open_dau_distribution as d 
		inner join public.register_unverifieduserapp as m on d.mobile_no = m.mobile
		where d.user_type = 'unverified' and created_datetime =var_date;
	
		-- raise notice ' inserted into test.naz_test_app_open_dau_distribution_v1 for: %',var_date;
	
		delete from tallykhata.app_open_dau_distribution_final
		where report_date=var_date;
	
		insert into tallykhata.app_open_dau_distribution_final
		select 
			s.created_datetime as report_date,
			'Using DAU' as remarks,
			case 
				when (s.created_datetime - s.reg_date::date )/7 <=3 then 'Reg in 3 weeks'
				when (s.created_datetime - s.reg_date::date)/7 >=4 and (s.created_datetime - s.reg_date::date)/7 <=6 then 'Reg in 4 to 6 weeks'
				when (s.created_datetime - s.reg_date::date)/7 >=7 and (s.created_datetime - s.reg_date::date)/7 <=9 then 'Reg in 7 to 9 weeks'
				when (s.created_datetime - s.reg_date::date)/7 >=10 then 'Reg in more than 10 weeks' end as category,
			case 
				when (s.created_datetime - s.reg_date::date)/7 <=3 then 1
				when (s.created_datetime - s.reg_date::date)/7 >=4 and (s.created_datetime - s.reg_date::date)/7 <=6 then 2
				when (s.created_datetime - s.reg_date::date)/7 >=7 and (s.created_datetime - s.reg_date::date)/7 <=9 then 3
				when (s.created_datetime - s.reg_date::date)/7 >=10 then 4 end as order_no,
			count(distinct s.mobile_no) as total_user
		from test.naz_test_app_open_dau_distribution_v1 as s
		group by s.created_datetime,category,order_no;
	
		commit; 
		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if; 
	end loop; 

end $$; 

-- see if matches
select *, cohort_txn_daus-dau diff
from 
	(select report_date, sum(total_user) cohort_txn_daus
	from tallykhata.app_open_dau_distribution_final
	where report_date >= '2020-07-01' and report_date <current_date
	group by 1
	) tbl1 
	
	inner join
			
	(-- dashboard DAU
	select 
		tbl_1.report_date,
		tbl_1.total_active_user_db_event dau
	from 
		(
		select 
			d.report_date,
			'T + Event [ DB ]' as category,
			sum(d.total_active_user) as total_active_user_db_event
		from tallykhata.tallykhata.daily_active_user_data as d 
		where d.category in('db_plus_event_date','Non Verified')
		group by d.report_date
		) as tbl_1 
	) tbl2 using(report_date)
where 
	1=1
	and report_date>='2022-02-01'
	-- and cohort_txn_daus>dau
order by 1; 
