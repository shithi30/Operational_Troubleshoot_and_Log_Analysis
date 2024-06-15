/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=299123227
- Data: 
- Function: 
- Table: data_vajapora.daily_unverfied_pus
- File: 
- Path: 
- Presentation: 
- Email thread: 
- Notes (if any): 
*/

do $$

declare
	var_date date:='2021-04-01'::date; 
begin 
	
	delete from data_vajapora.daily_unverfied_pus 
	where pu_date>=var_date;

	raise notice 'New OP goes below:'; 

	loop
	
		insert into data_vajapora.daily_unverfied_pus
		select 
			app_id, 
			count(unverified_user_activity_date) unverified_active_dates_last_30_days, 
			var_date pu_date
		from 
			(-- unverified users' last 30 days' TRT
			select app_id, date(create_date) unverified_user_activity_date
			from public.sync_unverifieduserjournal
			where date(create_date)>var_date-30 and date(create_date)<=var_date 
			
			union 
		
			-- unverified users' last 30 days' TACS
			select app_id, date(create_date) unverified_user_activity_date
			from public.sync_unverifieduseraccount 
			where 
				type in(2, 3)
				and date(create_date)>var_date-30 and date(create_date)<=var_date 
			) tbl1
		group by 1
		having count(unverified_user_activity_date)>=10; 
	
		raise notice 'Data generated for: %', var_date; 
	
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;

	end loop; 

end $$; 

/*	
truncate table data_vajapora.daily_unverfied_pus; 

select *
from data_vajapora.daily_unverfied_pus; 

select pu_date, count(app_id) unverified_pus
from data_vajapora.daily_unverfied_pus
group by 1
order by 1;
*/

