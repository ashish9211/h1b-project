h1b_pig= load '/home/hduser/new2/h1b' using PigStorage ('\t') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray,worksite:chararray,longitude:double,latitude:double);

filterbyyear= filter h1b_pig by $7 matches '2011';
filterbyjob= group filterbyyear by $4;
year11= foreach filterbyjob generate group,COUNT($1);

filterbyyear= filter h1b_pig by $7 matches '2012';
filterbyjob= group filterbyyear by $4;
year12= foreach filterbyjob generate group,COUNT($1);


filterbyyear= filter h1b_pig by $7 matches '2013';
filterbyjob= group filterbyyear by $4;
year13= foreach filterbyjob generate group,COUNT($1);

filterbyyear= filter h1b_pig by $7 matches '2014';
filterbyjob= group filterbyyear by $4;
year14= foreach filterbyjob generate group,COUNT($1);

filterbyyear= filter h1b_pig by $7 matches '2015';
filterbyjob= group filterbyyear by $4;
year15= foreach filterbyjob generate group,COUNT($1);

filterbyyear= filter h1b_pig by $7 matches '2016';
filterbyjob= group filterbyyear by $4;
year16= foreach filterbyjob generate group,COUNT($1);

allyears= join year11 by $0,year12 by $0,year13 by $0,year14 by $0,year15 by $0,year16 by $0;
allyears_final= foreach allyears generate $0,$1,$3,$5,$7,$9,$11;

growth= foreach allyears_final generate $0,(float)($6-$5)*100/$5,(float)($5-$4)*100/$4,(float)($4-$3)*100/$3,(float)($3-$2)*100/$2,(float)($2-$1)*100/$1;
avggrowth= foreach growth generate $0,($1+$2+$3+$4+$5)/5;
orderavggrowth= order avggrowth by $1 desc;

ans1b= limit orderavggrowth 5; 
