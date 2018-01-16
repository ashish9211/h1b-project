
h1b_pig= load '/home/hduser/h1b' using PigStorage ('\t') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray,worksite:chararray,longitude:double,latitude:double);

checknull= filter h1b_pig by $1 is not null and $1!='NA';
groupbyemployer= group checknull by $2;
totalall= foreach groupbyemployer generate group,COUNT(checknull.$1); 

checkcertified= filter h1b_pig by $1 =='CERTIFIED';
groupbyemployer1= group checkcertified by $2;
totalcertified= foreach groupbyemployer generate group,COUNT(checkcertified.$1);

checkcertifiedwd= filter h1b_pig by $1 =='CERTIFIED-WITHDRAWN';
groupbyemployer2= group checkcertifiedwd by $2;
totalcertifiedwd= foreach groupbyemployer2 generate group,COUNT(checkcertifiedwd.$1);


totalsjoin= join totalcertified by $0,totalcertifiedwd by $0,totalall by $0;

totalscount= foreach totalsjoin generate $0,$1,$3,$5;

temp1= foreach totalscount generate $0,(float)($1+$2)*100/($3),$3;
temp2= filter temp1 by $1>70 and $2>1000;
ans9= order temp2 by $1 DESC;
dump ans9;
