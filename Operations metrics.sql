create database project2;
use project2;
create table job_data(
ds DATE,
job_id int,
actor_id INT not null,
event varchar(100),
language varchar(100),
time_spent int not null,
org varchar(50));


ALTER TABLE job_data
modify column ds DATE;



insert into job_data(ds,job_id,actor_id,event,language,time_spent,org)
 values ('2020-11-30',21,1001,'skip','English',15,'A'),
('2020-11-30',22,1006,'transfer','Arabic',25,'B'),
('2020-11-29',23,1003,'decision','Persian',20,'C'),
('2020-11-28',23,1005,'transfer','Persian',22,'D'),
('2020-11-28',25,1002,'decision','Hindi',11,'B'),
('2020-11-27',11,1007,'decision','French',104,'D'),
('2020-11-26',23,1004,'skip','Persian',56,'A'),
('2020-11-25',20,1003,'transfer','Italian',45,'C');

#Case study(job data)
#1 number of jobs reviewed per hr per day of nov 2020
select
count(distinct job_id)/(30*24) as num_of_jobreview
from job_data
where
ds between '2020-11-01' and '2020-11-30';

#2throughput
select ds, jobs_reviewed,
avg(jobs_reviewed) over (order by ds rows between 6 preceding and current row)
as throughput_7_rolling_avg
from
(select ds, count(distinct job_id) as jobs_reviewed
 From job_data 
 where ds between '2020-11-01' and '2020-11-30'
group by ds
order by ds
)a;

#3 language share analysis
#percentage share of each language over last 30 days
select language, num_jobs, 
100.0* num_jobs/total_jobs as pct_share_jobs 
from(
select language, count(distinct job_id) as num_jobs 
from job_data 
group by language) a
cross join(
select count(distinct job_id) as total_jobs 
from job_data )b;


#4duplicate row detection
#identify duplicate rows in the data
select * from(
select *,
row_number()over(partition by job_id) as rownum
from job_data
 )a
where rownum>1;


#Case study 2 --- Metric Analysis
#preparation
use project2;

#Table 1 users
create table users(
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50));

select * from users;
use project2;

create table events(
user_id int,
occured_at varchar(100),
event_type varchar(100),
event_name varchar(100),
location varchar(50),
device varchar(50),
user_type int);


create table email_events(
user_id int,
occured_at varchar(100),
action varchar(100),
user_type int);

# case study 2
#1)user engagement
select
	extract(week from occured_at) as num_week, 
	count(distinct user_id) as no_of_distinct_user
from events group by num_week;

#2)user growth
select year, num_week, num_active_users,
 sum(num_active_users) over (order by year, num_week rows between unbounded
 preceding and current row) 
 as cumm_active_users
from
(select
extract(year from a.activated_at) as year, 
extract(week from a.activated_at) as num_week, 
count(distinct user_id) as num_active_users
from users a
where state='active'
group by year, num_week order by year, num_week
)a;


#3)weekly retention
select count(user_id),
sum(case when retention_week =1 then 1 else 0 end) as
per_week_retention

from(
select a.user_id,
a.sign_up_week,
b.engagement_week,
b.engagement_week-a.sign_up_week as retention_week

from
(
 (select distinct user_id, extract(week from occured_at) as sign_up_week
from events 
where event_type ='signup_flow'
and event_name ='complete_signup' 
and extract(week from occured_at)=18) a
left join
(select distinct user_id, extract(week from occured_at) as engagement_week
from events
where event_type = 'engagement')b 
on a.user_id = b.user_id
)
group by user_id
order by user_id;

#4) weekly engagement
select
extract(year from occured_at) as year_num,
extract(week from occured_at) as week_num,
device,
count(distinct user_id) as no_of_users
from events
where event_type='engagement'
 group by 1,2,3
order by 1,2,3;

#5) email engagement
select
100.0* sum(case when email_cat='email_opened' then 1 else 0 end) 
/sum(case when email_cat='email sent' then 1 else 0 end) as email_opening_rate,
100.0*sum(case when email_cat = 'email_clicked' then 1 else 0 end)
 /sum(case when email_cat='email_sent' then 1 else 0 end)
 as email_clicking_rate
from
(select *,
case when action in ('sent weekly digest', 'sent_reengagement_email')
then 'email_sent'
when action in ('email_open')
then 'email_opened'
when action in ('email clickthrough')
then 'email clicked'
end as email_cat
from events
)a;






