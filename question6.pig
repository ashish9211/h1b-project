h1b_pig= load '/home/hduser/h1b' using PigStorage ('\t') as (s_no:int,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:int,year:chararray,worksite:chararray,longitude:double,latitude:double);

groupbyyr= group h1b_pig by year; 
groupcsbyyr= foreach groupbyyr generate group as year,h1b_pig.$1;    
temp= foreach groupcsbyyr generate year,FLATTEN($1);
split temp into bagc if null::case_status =='CERTIFIED',bagw if null::case_status =='WITHDRAWN',bagcw if null::case_status =='CERTIFIED-WITHDRAWN',bagd if nul::case_status =='DENIED'; 

groupcertifiedbyyr= group bagc by year;
groupwdbyyr= group bagw by year;
groupcwdbyyr= group bagcw by year;
groupdeniedbyyr= group bagd by year;

certified= foreach groupcertifiedbyyr generate group as year,COUNT(bagc.$1);
withdrawn= foreach groupwdbyyr generate group as year,COUNT(bagw.$1); 
cwd= foreach groupcwdbyyr generate group as year,COUNT(bagcw.$1);
denied= foreach groupdeniedbyyr generate group as year,COUNT(bagd.$1);

joinallcs= join certified by $0,withdrawn by $0,cwd by $0,denied by $0;
finaljoin= foreach joinallcs generate $0,$1,$3,$5,$7,$1+$3+$5+$7;

ans6= foreach finaljoin generate $0, (double)$1/$5*100, (double)$2/$5*100, (double)$3/$5*100, (double)$4/$5*100;
dump ans6;
