use ig_clone;
select * from users;
# 1) Loyal user reward
select username ,created_at from users
order by created_at limit 5;

#2) inactive user engagement
select * from photos,users;
select * from users u left join photos p on p.user_id=u.id
where p.image_url is null order by username;

#3)contest winner declaration
select * from users,likes,photos;
select likes.photo_id,users.username,count(likes.user_id) as likesgot
from likes inner join photos on likes.photo_id=photos.id
inner join users on photos.user_id=users.id group by
likes.photo_id,users.username order by likesgot desc;

#limit 1;


#4)hashtag research
select * from tags,photo_tags;
select tags.tag_name,count(photo_tags.photo_id) as hashtag from photo_tags inner join 
tags on tags.id=photo_tags.tag_id group by tags.tag_name order by hashtag desc;
#limit 5;

#5)ad campaign launch
select * from users;
select DATE_FORMAT((created_at),'%W') as dayy ,count(username)
 from users group by 1 order by 2 desc;


#Inverstor metrics
#user engagement
select * from photos,users;
with base as(
select u.id as userid,count(p.id) as photoid from users u 
left join photos p on p.user_id=u.id group by u.id)
select sum(photoid) as totalphotos,count(userid) as total_users,
sum(photoid)/count(userid) as photoperuser
from base;


#2)bots and fake accounts
select * from users,likes;
with base as (
select u.username,count(l.photo_id) as likesgiven from  likes l 
inner join users u on u.id=l.user_id
group by u.username)
select username,likesgiven from base where likesgiven=(select count(*) from photos) 
order by username;





