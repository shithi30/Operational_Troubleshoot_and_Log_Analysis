/*
- Viz: https://datastudio.google.com/u/2/reporting/72e9308f-7d7e-45e6-931b-8b482fb8aeab/page/2TETC
- Data: 
- Function: data_vajapora.fn_group_wise_daily_avg_engagement_time()
- Table: data_vajapora.group_wise_daily_avg_engagement_time
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): 
*/

CREATE OR REPLACE FUNCTION data_vajapora.fn_group_wise_daily_avg_engagement_time()
 RETURNS void
 LANGUAGE plpgsql
AS $function$

/*
Authored by             : Shithi Maitra
Supervised by           : Md. Nazrul Islam
Purpose                 : Analysis of daily engagement time (in minutes) for different user-groups
Date of development     : 03-Aug-21
Version                 : 01
Auxiliary data table(s) : none
Target data table(s)    : data_vajapora.group_wise_daily_avg_engagement_time
*/

declare
	v_date date:=current_date-5;
begin
	-- Block-01: deleting backdated info. 
	delete from data_vajapora.group_wise_daily_avg_engagement_time
	where event_date>=v_date; 

	-- Block-02: identifying recent roamers
	execute 'select * from tallykhata.fn_roaming_users()'; 
	
	-- Block-03: inserting daily avg. engagement time in minutes
	raise notice 'New OP goes below:'; 
	loop
		insert into data_vajapora.group_wise_daily_avg_engagement_time
		select 
			event_date, 
			avg(case when roaming_date is not null then sec_with_tk else null end)/60.00 as roaming_dau_avg_min_with_tk,
			avg(sec_with_tk)/60.00 as dau_avg_min_with_tk,
			avg(case when rau_3_date is not null then sec_with_tk else null end)/60.00 as rau_3_avg_min_with_tk,
			avg(case when rau_10_date is not null then sec_with_tk else null end)/60.00 as rau_10_avg_min_with_tk,
			avg(case when fau_date is not null then sec_with_tk else null end)/60.00 as fau_avg_min_with_tk,
			avg(case when pu_date is not null then sec_with_tk else null end)/60.00 as pu_avg_min_with_tk
		from 
			(select event_date, mobile_no, sec_with_tk
			from tallykhata.daily_times_spent_individual_data
			where event_date=v_date
			) tbl1 
			
			left join 
			
			(select rau_date as rau_10_date, mobile_no as rau_10_mobile_no
			from tallykhata.tallykahta_regular_active_user_new
			where 
				rau_category=10
				and rau_date=v_date
			) tbl2 on(tbl1.mobile_no=tbl2.rau_10_mobile_no and tbl1.event_date=tbl2.rau_10_date)
			
			left join 
			
			(select rau_date as rau_3_date, mobile_no as rau_3_mobile_no
			from tallykhata.tallykhata_regular_active_user
			where 
				rau_category=3
				and rau_date=v_date
			) tbl3 on(tbl1.mobile_no=tbl3.rau_3_mobile_no and tbl1.event_date=tbl3.rau_3_date)
			
			left join 
			
			(select report_date as fau_date, mobile as fau_mobile_no
			from tallykhata.fau_for_dashboard
			where 
				category in('fau', 'fau-1')
				and report_date=v_date
			) tbl4 on(tbl1.mobile_no=tbl4.fau_mobile_no and tbl1.event_date=tbl4.fau_date)
			
			left join 
			
			(select roaming_date, mobile_no as roamer_mobile_no
			from tallykhata.roaming_users
			where roaming_date=v_date
			) tbl5 on(tbl1.mobile_no=tbl5.roamer_mobile_no and tbl1.event_date=tbl5.roaming_date)
			
			left join 
			
			(select distinct report_date as pu_date, mobile_no as pu_mobile_no
			from tallykhata.tk_power_users_10
			where report_date=v_date
			) tbl6 on(tbl1.mobile_no=tbl6.pu_mobile_no and tbl1.event_date=tbl6.pu_date)
		group by 1; 
		raise notice 'Data generated for: %', v_date; 
	
		-- controlling loop for generating data till yesterday
		v_date:=v_date+1;
		if v_date=current_date then exit;
		end if; 
	end loop;

END;
$function$
;

/*
select data_vajapora.fn_group_wise_daily_avg_engagement_time(); 

select *
from data_vajapora.group_wise_daily_avg_engagement_time; 
*/
