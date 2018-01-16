h1b_pig= load '/home/hduser/h1b' using PigStorage ('\t') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray,worksite:chararray,longitude:double,latitude:double);


filterbycs= filter h1b_pig by $1 is not null and $1!='NA';
temp= group filterbycs by $4;
totalall= foreach temp generate group,COUNT(filterbycs.$1);

certified= filter h1b_pig by $1 == 'CERTIFIED';
temp1= group certified by $4;
totalcertified= foreach temp1 generate group,COUNT(certified.$1);

certifiedwithdrawn= filter h1b_pig by $1 == 'CERTIFIED-WITHDRAWN';
temp2= group certifiedwithdrawn by $4;
totalcertifiedwithdrawn= foreach temp2 generate group,COUNT(certifiedwithdrawn.$1);

joinall= join totalcertified by $0,totalcertifiedwithdrawn by $0,totalall by $0;
final= foreach joinall generate $0,$1,$3,$5;
temp1= foreach final generate $0,(float)($1+$2)*100/($3),$3;
temp2= filter temp1 by $1>70 and $2>1000;
ans10= order temp2 by $1 DESC;
dump ans10;
