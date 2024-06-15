/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=2009034645
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
	>> how many user sent registration request as new user
	>> how many user successfully registered as OTP verified user
	>> how many user using as unverified
	>> how many user shared their business name
	>> how many user recorded at least 1 transaction
*/

-- from live to data_vajapora.register_tallykhatauser_2
select mobile_no
from public.register_tallykhatauser
where 
    shop_name is not null
    and shop_name!=''
    and mobile_no is not null;

do $$ 

declare 
	var_date date:=current_date-21;
begin 
	raise notice 'New OP goes below:'; 

	loop 
		-- new users requested registration
		drop table if exists data_vajapora.help_a;
		create table data_vajapora.help_a as
		select user_id 
		from public.eventapp_event
		where
			event_name='/api/auth/init' 
			and message='response generated for new user'
			and created_at::date=var_date; 
		
		insert into data_vajapora.init_reg_stats
		select var_date report_date, *
		from 
			(select count(distinct user_id) reg_request_users
			from data_vajapora.help_a
			) tbl1,
			
			(select count(distinct mobile_number) otp_verified_regs
			from 
				public.register_usermobile tbl1 
				inner join 
				data_vajapora.help_a tbl2 on(tbl1.mobile_number=tbl2.user_id)
			) tbl2,
			
			(select count(mobile_no) unverified_users
			from 
				(select distinct user_id mobile_no
				from data_vajapora.help_a
				) tbl1 
				
				inner join 
				
				(select distinct mobile mobile_no 
				from public.register_unverifieduserapp 
				) tbl2 using(mobile_no)
				
				left join 
				
				(select mobile_number mobile_no 
				from public.register_usermobile
				) tbl3 using(mobile_no)
			where tbl3.mobile_no is null
			) tbl3,
			
			(select count(mobile_no) gave_shop_name
			from 
				(select distinct user_id mobile_no
				from data_vajapora.help_a
				) tbl1 
				
				inner join 
				
				(-- from live
				select mobile_no 
				from data_vajapora.register_tallykhatauser_2
				) tbl2 using(mobile_no)
			) tbl4,
			
			(select count(distinct mobile_no) at_least_1_txn
			from 
				(select user_id mobile_no
				from data_vajapora.help_a
				) tbl1 
				
				inner join 
				
				(select mobile_no
				from tallykhata.tallykhata_transacting_user_date_sequence_final
				where created_datetime>=var_date and created_datetime<current_date
				) tbl2 using(mobile_no)
			) tbl5; 

		raise notice 'Data generated for: %', var_date; 
		var_date:=var_date+1;
		if var_date=current_date then exit;
		end if;
	end loop; 
	
end $$; 

select *
from data_vajapora.init_reg_stats; 
