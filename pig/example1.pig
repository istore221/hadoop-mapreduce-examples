--pig -x local mypig.pig

applogs  = LOAD 'logs' using PigStorage('|') AS (classname:chararray,loglevel:chararray);

loglevels  = foreach applogs generate loglevel;

distinctloglevels = DISTINCT loglevels;

filter1  = FILTER distinctloglevels BY loglevel  == 'INFO';

filter2 =  FILTER applogs BY $1 eq 'INFO';

filter3 = FILTER applogs BY $1 IN ('INFO','DANGER');

filter4 = FILTER applogs BY $1 == 'INFO' OR $1 == 'DANGER';

limit1 =  LIMIT applogs 5;

SPLIT applogs INTO infos IF $1 eq 'INFO', danger  IF $1 eq 'DANGER';

sample1  = SAMPLE applogs  0.1;

order1 = ORDER applogs BY loglevel ASC;

groupapplogs  = GROUP applogs BY loglevel;

groups = foreach groupapplogs generate group;

groups2 = foreach groupapplogs generate group,COUNT(applogs);

cubedinp = CUBE applogs BY CUBE(applogs.loglevel);

DUMP cubedinp;
