create table job_info(
id int auto_increment primary key,
job_query varchar(30),
title varchar(200),
company varchar(50),
location varchar(80),
description text
)charset=utf8;