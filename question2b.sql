--2 b) find top 5 locations in the US who have got certified visa for each year.[certified]

 select worksite, total, year from( select worksite, count(case_status) as total, year from h1b where case_status='CERTIFIED' and year='2011' group by worksite,year order by total desc limit 5)sub1
 union
 select worksite, total, year from( select worksite, count(case_status) as total, year from h1b where case_status='CERTIFIED' and year='2012' group by worksite,year order by total desc limit 5)sub2
 union
 select worksite, total, year from( select worksite, count(case_status) as total, year from h1b where case_status='CERTIFIED' and year='2013' group by worksite,year order by total desc limit 5)sub3
 union
 select worksite, total, year from( select worksite, count(case_status) as total, year from h1b where case_status='CERTIFIED' and year='2014' group by worksite,year order by total desc limit 5)sub4
 union
 select worksite, total, year from( select worksite, count(case_status) as total, year from h1b where case_status='CERTIFIED' and year='2015' group by worksite,year order by total desc limit 5)sub5
 union
 select worksite, total, year from( select worksite, count(case_status) as total, year from h1b where case_status='CERTIFIED' and year='2016' group by worksite,year order by total desc limit 5)sub6
 order by year;
  
