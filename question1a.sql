select year, count(job_title) from h1b where job_title like '%DATA ENGINEER%' group by year order by year;
