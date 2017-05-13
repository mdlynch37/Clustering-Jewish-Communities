recode  denom (sysmis = -1)(1=1)(2=2)(3=3)(4=4)(5=5)(6=6)(7=7)(8=8)(9=9)(10=10)(11=11)(12=12) into denomall.
recode  friserve satabser satanser (10 thru hi=-1)/ friatt satabatt satanatt (9999 thru hi =-1)/ 
WORSH10F WORSH10S WORSH09F WORSH09S WORSH08F WORSH08S worsh07F worsh07S worsh06F worsh06S worsh05F worsh05S (0 1500 thru hi=-1)/
 memunit members (9999 15000 thru hi =-1)/  natams to multi (8000 thru hi =-1)/  
seniors middleag medadult yadults kidyth females COLLEGE NEWMEMB LIFELONG NEARBY (101 thru hi=-1)/EVERYONE (10000 THRU HI=-1)/ rabbiwed mixed (200 THRU HI=-1)/
BUDGET (lo thru 200 9999999 thru hi=-1)/ ZIPCODE (0 99999 thru hi=-1) / seating (lo thru 49 9999 thru hi=-1).

val labels denomall 2 'Conservative' 3'Reform'.

missing values  denomall friserve satabser satanser friatt satabatt satanatt 
WORSH10F WORSH10S WORSH09F WORSH09S WORSH08F WORSH08S worsh07F worsh07S worsh06F worsh06S worsh05F worsh05S
 memunit members  natams to multi  seniors middleag medadult yadults kidyth females COLLEGE NEWMEMB LIFELONG NEARBY  EVERYONE
 rabbiwed mixed   budget  zipcode   seating( -1).
missing values  white (0,-1)/  FULLORD PARTORD (50).
recode  choir to thought  recruit  GOODPRAY to PARENTS  family to JUSTICE (4 5=100)(1 2 3=0).
recode  sschool to  ISRAELACT EVENTS (3 4 = 100)(1 2=0)/ novisit to bymater (1=100)(0=0) .
count soupkit =  SOUP_Y1 SOUP_Y2 (1).
count cashhelp= CASH_Y1 CASH_Y2 (1).
count daycare2= DCARE_Y1 DCARE_Y2 (1).
count tutor2= TUTOR_Y1 TUTOR_Y2 (1).
count healthed=  HEALTHY1 HEALTHY2 (1).
count comorgnz= COMORGY1 COMORGY2 (1).
count jobtrain= JOBED_Y1 JOBED_Y2 (1).
count finaned= FINED_Y1 FINED_Y2 (1).
count oldprogm=  OLDPROY1 OLDPROY2 (1).
count votered= VOTEEDY1 VOTEEDY2  (1).
count immigprg=IMMPROY1 IMMPROY2 (1).
recode soupkit to  votered (1 2 =100).
recode  leadwors leadvis doevan dotrain docare doteach dosmall doadmin dorepres WORKYADS dealargu(1 2=0)(3 4=100).
count rabbiRes= role (1 2 3).
count conffin=  concash2 concash3 concash4 (1).
count confwor=   conwors2 conwors3 conwors4  (1).
count confprog=   conprog2 conprog3 conprog4  (1).
count confsyn=   consyn2 consyn3 consyn4  (1).
count conflead=   conlead2 conlead3 conlead4  (1).
count conflact=   CONLACT2 CONLACT3 CONLACT4  (1).
count confbhvr=   CONMACT2 CONMACT3 CONMACT4  (1).
count conffacl=   confac2 confac3 confac4 (1).
recode conffin to conffacl (1 2 3=100).
recode  LAYOFFS PAYCUTS DELAYS INVEST MISSION CAPITAL COUNSEL CASHNEED HOUSNEED NOWORK (1 2=0)(3 4=100).

compute member4=memunit.

recode member4 (1 thru 249=1)(249 thru 499=3)(499 thru 749=5)(749 thru 4000=7).
missing values member4 (-1).
val labels member4 1 'Under 250 member units' 3'250-499' 5 '500-749' 7'750+'.
compute memdenom=member4+(denomall-2).
value labels memdenom 1 'Con LT 250' 2'Ref LT 250' 3'C 250-499' 4'R 250-499' 5'C 500-749' 6'R 500-749' 7'C 750+' 8'R 750+'.

compute hhsize = everyone/memunit.
compute hhratio=memdenom.
recode hhratio (1=1.9)(2=2.4)(3=2.1)(4=3.1)(5=2.2)(6=3.5)(7=2.6)(8=2.9).
compute everyone2=everyone.
if (sysmis (everyone2))everyone2=memunit*hhratio.
if (friatt gt everyone2)everyone2=friatt.
if (satabatt gt everyone2)everyone2=satabatt.
recode everyone2 (lo thru 25 9999 = -1).
missing values everyone2 (-1). 
compute memun50=memunit.
recode memun50 (lo thru 49=-1).
missing values memun50 (-1).
compute  rfriatt=friatt/memun50.
compute rsatabatt= satabatt/memun50.
compute rsatanatt= satanatt/memun50.
var lab rfriatt 'Fri attendance per 100 families' /rsatabatt 'Sat Bar Mitz attend per 100 fam'/ rsatanatt 'Sat no BarM attend per 100 fam'.
compute rrabwed= rabbiwed/memun50.
compute rmixed= mixed/memun50.
compute rmixwed=mixed/rabbiwed.
if (rabbiwed eq 0)rmixwed=-1.
if (rabbiRes ne 1 or denomall eq 2)rmixwed=-1.
if (rmixwed gt 1)rmixwed=-1.
missing values rmixwed (-1). 
compute rrabbi=( FULLORD+(.5* PARTORD))/memun50.
compute rcantor=( FULLCANT+(.5* PARTCANT))/memun50.
compute redu= (FULLEDU +(.5*PARTEDU))/memun50.
compute totwork=  (FULLORD+ FULLCANT+ FULLEDU+ FULLADM+ FULLPROG+ FULLSEC+ FULLCUST+ FULLOTH+ 
(.5*( PARTORD+ PARTCANT+ PARTADM +PARTPROG +PARTEDU+ PARTSEC+ PARTCUST+ PARTOTH))).
compute rtotwork=totwork/memun50.
var lab rrabbi 'Rabbis per 100 families'/rcantor 'Cantors per 100 families' / Redu 'Educators per 100 families' / totwork 'Total number of workers' / rtotwork
 'Workers per 100 families'.
do repeat x= rfriatt rsatabatt rsatanatt rrabwed rmixwed rrabbi rcantor redu rtotwork.
compute x=100*x.
end repeat.
compute rbudget=(budget/memun50).
var labels rbudget 'Dollars per family'.

compute weightm= memunit/462.

compute musical=( drums + ELECTRIC)/2.
compute insprtnl=( joyful +INNOVATE+ INSPIRE+ thought)/4.
compute recruitx= ( bymail+ byphone+ byemail+ bymater)/4.
compute socljust=(SOUP_Y1+  CASH_Y1 +TUTOR_Y1+  HEALTHY1 + COMORGY1+  JOBED_Y1+  FINED_Y1+  OLDPROY1+ VOTEEDY1+ IMMPROY1)*10.
compute relgemph = (GOODPRAY +STUDYBIB+ GOODHOLY)/3.
compute morale=( vital +purpose+ BEACON +change+ BELIEFS)/5.
var labels relgemph 'Emphasis on religious themes Index' / morale 'Morale Index'.
compute socljus2= (soupkit+ cashhelp + tutor2+ healthed +comorgnz +jobtrain+ finaned+ oldprogm+ immigprg+ votered)/10.
compute confld2= (conflead +conflact)/2.
compute confinst= (conffin+ confwor +confprog+ confsyn+ conffacl)/5.
compute econeed =  (COUNSEL +CASHNEED+ HOUSNEED +NOWORK)/4.
compute ecostaff = (LAYOFFS+ PAYCUTS +DELAYS)/3.
compute ecomoney= (INVEST +MISSION+ CAPITAL)/3.
var lab recruitx 'Recruitment activity index'/econeed 'Impact of downturn on members needs Index'/
ecostaff 'Impact of downturn on staff Index'/ ecomoney 'Impact of downturn on cash available Index'.
compute zip3= ZIPCODE/100.
compute zip3=trunc (zip3).
compute areas7=zip3.
recode areas7 (100 thru 119=1)(700 thru 999 =7)(300 thru 399=5)(400 thru 699=4)(150 thru 299=7)
  (9 thru 69 =3)(120 thru 149 70 thru 89=2).
value labels areas7 1 'NY area' 7'West & Mountain' 5'South' 4'Midwest'  3'New England'
 2'Northeeast & Atlantic Seaboard'.

count metro = place (4 5 6 7).
recode metro (1=100).

compute financhg= finances + 10*FINAN05.
compute financh2=financhg.
recode financh2 (54 43 32 21 =2)(53 52 51 42 41 31 =1)(55 44 33 22 11=3)(45 34 23 12=4)( 35 24 25 13 14 15 = 5).
var lab financh2 'Financial change over 5 years'. 
val lab financh2 1'much worse' 2'worse' 3'same' 4'better' 5'much better'.

compute zipdenom=(10* ZIPCODE)+ memdenom.
compute duplicates=zipdenom.
var labels duplicates 'Duplicate status'.
val labels duplicates 0'Keep' 1'Duplicates' 2'Drop'.
recode duplicates (600357 88731 100033
940228 20621
70067
70163
76663
80037
106057
190665
190727
191195
208521
212087
24661
85405
100658
109403
110237
113663
117581
117763
132143
152203
190383
190723
191238
198023
201751
208513
212088
212091
235175
303603
342375
352055
381208
443203
600153
600158
600627
601486
770968
787315
802377
802378
900648
914367
940613
945498
12013
14531
18304
19301
20212
20382
24215
24923
26014
29067
30643
61111
64571
64773
68305
70397
70415
70795
70813
70908
74052
74174
76616
77313
80437
82031
86483
88073
88405
89043
100758
104713
105283
105661
105982
109625
113641
113755
115185
115422
115543
115777
117313
117433
117533
117765
117917
119631
123093
126013
126033
144671
152177
166022
183502
183601
190028
190201
190277
190831
191035
191523
193552
199011
200168
201472
201713
208321
208508
208527
208863
209023
210443
220394
223025
232206
234511
277013
296152
322178
326053
330243
333265
334341
334726
335112
336122
338031
352225
363032
379191
381203
383012
402418
405021
430544
432057
432136
447091
452368
452491
462608
464032
480347
480844
481043
482374
483018
543011
551183
600626
600894
600914
601201
603024
606578
631088
631312
662096
708092
722124
731181
752257
767072
770967
787566
799024
799123
802072
802208
803031
804652
852836
857117
857166
900247
900468
900778
913075
913568
913677
925062
926911
941161
945662
945963
947023
950326
950601
954044
961482
981153
981158
981162
982012
992031 =1) (else=0).

compute leaderrole= role.
recode leaderrole (1=1)(11=2)(5=3)(else=4).
val lab leaderrole 1'Lead rabbi' 2'President' 3'Exec' 4'Other'.

compute ziprabbi= zipdenom.
if (duplicates ne 1)ziprabbi=0.
if (leaderrole ne 1)ziprabbi=3.
val lab ziprabbi 0'Not a duplicate' 3'Not a rabbi'.
if (leaderrole eq 1 and duplicates eq 1)duplicates=0.

compute rabbidrop=zipdenom.
recode rabbidrop
(212087
600914
18304
19301
20212
20382
24923
26014
30643
61111
70067
70415
70795
70908
74052
74174
76616
77313
82031
85405
100033
100758
104713
105283
105661
105982
106057
109403
113641
113663
113755
115422
115543
115777
117313
117433
117533
117763
117917
126033
152177
152203
183502
183601
190201
190277
190383
190723
191523
193552
198023
199011
201472
201751
208321
208521
208863
209023
210443
212088
220394
223025
232206
234511
235175
296152
303603
322178
326053
330243
334726
335112
336122
363032
381208
383012
405021
432057
443203
452368
452491
462608
464032
480347
480844
482374
483238
551183
600153
600158
600357
600626
601201
601486
631088
631312
662096
708092
722124
767072
770968
787315
799024
802072
802208
802378
804652
852836
857117
857166
900468
900648
913568
914367
925062
940228
940613
945498
945662
945963
947023
950326
954044
961482
982012 =2)(else=0).
if (rabbidrop=2 and duplicates=1)duplicates=2.

compute zippres= zipdenom.
if (duplicates ne 1)zippres=0.
if (leaderrole ne  2)zippres=3.
val lab zippres 0'Not a duplicate' 3'Not a pres'.
if (zippres gt 3 and duplicates eq 1)duplicates=0.

compute presdrop=zipdenom.
recode presdrop
(29067
64773
70813
117581
119631
123093
166022
191195
200168
201713
212091
333265
334341
352055
352225
381203
447091
543011
600627
731181
787566
900778
913075
913677
926911
981158
981162  =2)(else=0).
if (presdrop=2 and duplicates=1)duplicates=2.

compute zipexec=zipdenom.
if (duplicates ne 1)zipexec=0.
if (leaderrole ne 3)zipexec=3.
val lab zipexec 0'Not a duplicate' 3'Not an exec'.
if (zipexec gt 3 and duplicates eq 1)duplicates=0.

compute execdrop=zipdenom.
recode execdrop
(70163
70397
76663
80037
100658
110237
190028
190665
191238
208508
402418
430544
432136
483018
600894
603024
606578
752257
770967
802377
803031=2)(else=0).
if (execdrop=2 and duplicates=1)duplicates=2.

compute zipdupes=zipdenom.
if (duplicates ne 1)zipdupes=duplicates.
val lab zipdupes 0'Keep' 2'Drop'.

compute respondentrank= role.
recode respondentrank (1=12)(11=11)(5=10)(2 3=4)(4=3)(6 7 8 9=2)(10 12=1)(else=0).
compute zipdenomresp=(1000000*respondentrank)+zipdenom.

compute resprankdropkeep=zipdenomresp.
recode resprankdropkeep
(4190727 4208527 4900247 3068305 3080437 3088405 3338031 2012013 1014531 1024215 1024661 1064571 1089043 2117765 1190831 1208513 1481043 1941161 2950601
2981153 2064571=0)
(3190727 1190727 1208527 3900247 68305 80437 2088405 338031 1012013 14531 24215 24661 164571 89043 117765 190831 208513 481043 941161 950601
1981153 1342375=2)
(else=9).
if (duplicates eq 1 and resprankdropkeep lt 9)duplicates=resprankdropkeep.

compute wtresp=duplicates.
recode wtresp (0=1)(1=.5)(2=0). 
