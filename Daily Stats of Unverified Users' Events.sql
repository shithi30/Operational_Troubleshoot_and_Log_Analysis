/*
- Viz: https://docs.google.com/spreadsheets/d/17JJcWTdpJQgTih7iywy3A8Hngs3FFNtv9p5oaJB3BDM/edit#gid=838431219
- Data: 
- Function: 
- Table:
- File: 
- Path: 
- Document/Presentation: 
- Email thread: 
- Notes (if any): It's hard to tell at this point since we have just 4 days' data with 2 holidays, but it seems ~ 4k per day.
*/

select date(created_at) event_date, count(distinct unverified_app_id) unverified_users
from public.sync_appeventunverified
group by 1
order by 1; 