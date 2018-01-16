--5)a) for all the applications
 select job_title, total, year from( select job_title, count(case_status) as total, year from h1b where year='2011' group by job_title, year order by total desc limit 10)sub1
 union
 select job_title, total, year from( select job_title, count(case_status) as total, year from h1b where year='2012' group by job_title, year order by total desc limit 10)sub2
 union
 select job_title, total, year from( select job_title, count(case_status) as total, year from h1b where year='2013' group by job_title, year order by total desc limit 10)sub3
 union
 select job_title, total, year from( select job_title, count(case_status) as total, year from h1b where year='2014' group by job_title, year order by total desc limit 10)sub4
 union
 select job_title, total, year from( select job_title, count(case_status) as total, year from h1b where year='2015' group by job_title, year order by total desc limit 10)sub5
 union
 select job_title, total, year from( select job_title, count(case_status) as total, year from h1b where year='2016' group by job_title, year order by total desc limit 10)sub6
 order by year;
