/*
- Viz: 
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation/Dashboard: 
- Email thread: 
- Notes (if any): 
*/

select *
from 
	(select *
	from 
		(select max(id) journal_id
		from public.journal 
		) tbl1 
		
		inner join 
		
		(select id journal_id, create_date::timestamp journal_create_date
		from public.journal 
		) tbl2 using(journal_id)
	) tbl1,
	
	(select *
	from 
		(select max(id) account_id
		from public.account
		) tbl1 
		
		inner join 
		
		(select id account_id, create_date::timestamp account_create_date
		from public.account
		) tbl2 using(account_id)
	) tbl2, 
	
	(select *
	from 
		(select max(id) reg_usermobile_id
		from public.register_usermobile
		) tbl1 
		
		inner join 
		
		(select id reg_usermobile_id, created_at::timestamp reg_usermobile_create_date
		from public.register_usermobile
		) tbl2 using(reg_usermobile_id)
	) tbl3,
	
	(select *
	from 
		(select max(id) reg_users_id
		from public.registered_users
		) tbl1 
		
		inner join 
		
		(select id reg_users_id, created_at::timestamp reg_usermobile_create_date
		from public.registered_users
		) tbl2 using(reg_users_id)
	) tbl4, 
	
	(select *
	from 
		(select max(id) ledger_id
		from public.ledger 
		) tbl1 
		
		inner join 
		
		(select id ledger_id, create_date::timestamp ledger_create_date
		from public.ledger 
		) tbl2 using(ledger_id)
	) tbl5;
	