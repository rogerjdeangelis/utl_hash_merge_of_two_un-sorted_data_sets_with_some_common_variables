# utl_hash_merge_of_two_un-sorted_data_sets_with_some_common_variables
Efficient Hash merge of two un-sorted data sets with some common variables.  Keywords: sas sql join merge big data analytics macros oracle teradata mysql sas communities stackoverflow statistics artificial inteligence AI Python R Java Javascript WPS Matlab SPSS Scala Perl C C# Excel MS Access JSON graphics maps NLP natural language processing machine learning igraph DOSUBL DOW loop stackoverflow SAS community.
    Efficient Hash merge of two un-sorted data sets with some common variables

    This solution was provided by: Paul Dorfman <sashole@bellsouth.net>

    Benchmarks
    ==========
     6:47  Single Hash merging 24 million with 68 million
     2:58  Five parallel Hashes one per year (Year 2001-2005)

    github
    https://goo.gl/t7xvWd
    https://github.com/rogerjdeangelis/utl_hash_merge_of_two_un-sorted_data_sets_with_some_common_variables

    The Five tasks created five datasets, want2001-want2005, and I did not include
    the append. Actually having five files may help in subsequent processing.

    Parallel Solution is not quite fair. I was unable to determine the firstsobs and lastobs record numbers by year using a very
    fast binary serach(.05 seconds). My attempt is at the end, so I created an 'year' index on the two input files
    which took about 3 minutes in all.

    see
    https://goo.gl/uY4xpk
    https://github.com/rogerjdeangelis/utl_merging_two_sorted_large_data_sets_with_common_key


    see
    https://goo.gl/QyD9XG
    https://communities.sas.com/t5/General-SAS-Programming/Efficiently-Merging-Two-Large-Data-Sets-With-Some-Common/m-p/431464

    *_                   _
    (_)_ __  _ __  _   _| |_
    | | '_ \| '_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    ;

     OVERVIEW

        Using a Hash
        The first data set, home._2004, contains 45 variables and 67 million observations.
        The second data set, home.c2final, contains 73 variables and 24 million observations.
        Join using common  primary key below

                  DATASETS
          F._2004         G.C2FINA
          -------         --------

           Year         = Year
           MSA          = MSA
           CountyCode   = CountyCode
           state        = state
           Loan_Amount  = Loan_Amount

     HASH Algorithm

      1. Table 1: Load the 16 byte md5 hash of the concatenation of the primary keys along
         with '_n_' record number(rid) into in memory hash table.

      2. Table 2: Create the same md5 hash in the larger table. (and bring in the entire PDV)

      3. Lookup the Table 2: md5 hash(from 2.) in the in Table 1: memory hash table and
         when there is a match retrieve the record number(_n_) for Table 1.

      4. Use point=record number on Table 1 data and  retrieve the matching  record.


     F._2004  DATASET 46 variables Total Obs 68,000,500
     ====================================================

     F._2004  Middle Observation(34,000,250 ) of f._2004 - Total Obs 68,000,500

      Primary Key  (sorted 45 variables)

                                       VALUE

        YEAR                C  4       2003
        MSA                 C  1       B
        COUNTYCODE          C  2       16
        STATE               C  2       VT
        LOAN_AMOUNT         N  8       27200000


         -- CHARACTER --
        A1                  C 8       12345678
        A2                  C 8       12345678
        A3                  C 8       12345678
        ...                 ...       ........
        A20                 C 8       12345678


         -- NUMERIC --
        N1                  N 8       12345678
        N2                  N 8       12345678
        N3                  N 8       12345678
        ...                 ...       ........
        N20                 N 8       12345678


      G.C2FINA  dataset 73 varables Total Obs 24,000,000
      ==================================================

       Middle Observation(12000000 ) of g.c2final - Total Obs 24,000,000
                                       VALUE

       RYEAR               C 4       2003
       RMSA                C 1       B
       RCOUNTYCODE         C 2       16
       RSTATE              C 2       VT
       RLOAN_AMOUNT        N 8       27200000

          -- CHARACTER --
        B1                 C 8       12345678
        B2                 C 8       12345678
        B3                 C 8       12345678
        ...                 ...       ........
        B40                C 8       12345678

         -- NUMERIC --
        M1                 N 8       12345678
        M2                 N 8       12345678
        M3                 N 8       12345678
        ...                 ...       ........
        M28                N 8       12345678

    *
      proc sql;
        create
          table want as
        select
          l.*
         ,r.*
        from
          f._2004    as l
          ,g.c2final as r
        where
          l.Year         = r.rYear         and
          l.MSA          = r.rMSA          and
          l.CountyCode   = r.rCountyCode   and
          l.state        = r.rstate        and
          l.Loan_Amount  = r.rLoan_Amount
      ;quit;

    OUTPUT
    ======

     WORK.WANT 118 variables and 120,000 observations

       Middle Observation(60000 ) of want - Total Obs 120,000

       YEAR             C 4       2003
       MSA              C 1       B
       COUNTYCODE       C 2       16
       STATE            C 2       VT
       LOAN_AMOUNT      N 8       48000

       RYEAR            C 4       2003
       RMSA             C 1       B
       RCOUNTYCODE      C 2       16
       RSTATE           C 2       VT
       RLOAN_AMOUNT     N 8       48000

        -- CHARACTER --
       A1               C 8       12345678
       A2               C 8       12345678
       A3               C 8       12345678
        ...                 ...       ........
       A20              C 8       12345678

       B1               C 8       12345678
       B2               C 8       12345678
        ...                 ...       ........
       B3               C 8       12345678
       B40              C 8       12345678


        -- NUMERIC --
       N1               N 8       12345678
       N2               N 8       12345678
       N3               N 8       12345678
        ...                 ...       ........
       N20              N 8       12345678

       M1               N 8       12345678
       M2               N 8       12345678
       M3               N 8       12345678
        ...                 ...       ........
       M28              N 8       12345678

    *
     _ __  _ __ ___   ___ ___  ___ ___
    | '_ \| '__/ _ \ / __/ _ \/ __/ __|
    | |_) | | | (_) | (_|  __/\__ \__ \
    | .__/|_|  \___/ \___\___||___/___/
    |_|
    ;

      * SINGLE HASH;

       data want (drop = key) ;
         dcl hash h(hashexp:20, multidata:"y") ;
         h.definekey ("key") ;
         h.definedata ("rid") ;
         h.definedone() ;
         length key $ 16 ;
         do rid = 1 by 1 until (z1) ;
           set g.c2final ( keep=rYear--rLoan_Amount) end = z1 ;
           key = md5 (catx (":", of rYear--rLoan_Amount)) ;
           h.add() ;
         end ;
         do until (z2) ;
           set f._2004 end = z2 ;
           key = md5 (catx (":", of Year--Loan_Amount)) ;
           do _iorc_ = h.find() by 0 while (_iorc_ = 0) ;
             set g.c2final point = rid ;
             output ;
             _iorc_ = h.find_next() ;
           end ;
         end ;
         stop ;
       run;quit;

       NOTE: There were 24000000 observations read from the data set G.C2FINAL.
       NOTE: There were 68000500 observations read from the data set F._2004.
       NOTE: The data set WORK.WANT has 120000 observations and 118 variables.
       NOTE: DATA statement used (Total process time):
             real time           6:47.00
             cpu time            6:46.92

     * FIVE HASHES;

        %let _s=%sysfunc(compbl(C:\Progra~1\SASHome\SASFoundation\9.4\sas.exe -sysin c:\nul
          -sasautos c:\oto -autoexec c:\oto\Tut_Oto.sas -work d:\wrk));

        data _null_;file "c:\oto\utl_hshTsk.sas" lrecl=512;input;put _infile_;putlog _infile_;
        cards4;
        %macro utl_hshTsk(year);

           libname f "f:/wrk";
           libname g "g:/wrk";

           data c2final/view=c2final;
             set g.c2final(where=(ryear="&year"));
           run;quit;

           data want&year (drop = key) ;
             dcl hash h(hashexp:20, multidata:"y") ;
             h.definekey ("key") ;
             h.definedata ("rid") ;
             h.definedone() ;
             length key $ 16 ;
             do rid = 1 by 1 until (z1) ;
               set g.c2final (where=(ryear="&year") keep=rYear--rLoan_Amount) end = z1 ;
               key = md5 (catx (of rYear--rLoan_Amount)) ;
               h.add() ;
             end ;
             do until (z2) ;
               set f._2004(where=(year="&year")) end = z2 ;
               key = md5 (catx (of Year--Loan_Amount)) ;
               do _iorc_ = h.find() by 0 while (_iorc_ = 0) ;
                 set c2final point = rid ;
                 output ;
                 _iorc_ = h.find_next() ;
               end ;
             end ;
             stop ;
           run;quit;

        %mend utl_hshTsk;
        ;;;;
        run;quit;

        * check interactively;
        %include "c:/oto/utl_hshTsk.sas";
        %utl_hshTsk(2003);

        systask kill sys1 sys2 sys3 sys4  sys5 ;
        systask command "&_s -termstmt %nrstr(%utl_hshTsk(2001);) -log d:\log\a1.log" taskname=sys1;
        systask command "&_s -termstmt %nrstr(%utl_hshTsk(2002);) -log d:\log\a2.log" taskname=sys2;
        systask command "&_s -termstmt %nrstr(%utl_hshTsk(2003);) -log d:\log\a3.log" taskname=sys3;
        systask command "&_s -termstmt %nrstr(%utl_hshTsk(2004);) -log d:\log\a4.log" taskname=sys4;
        systask command "&_s -termstmt %nrstr(%utl_hshTsk(2005);) -log d:\log\a5.log" taskname=sys5;
        waitfor sys1 sys2 sys3 sys4  sys5;


        NOTE: There were 4800000 observations read from the data set G.C2FINAL.
              WHERE ryear='2001';
        NOTE: There were 4800000 observations read from the data set G.C2FINAL.
              WHERE ryear='2001';
        NOTE: There were 13600100 observations read from the data set F._2004.
              WHERE year='2001';
        NOTE: The data set WORK.WANT2001 has 24000 observations and 118 variables.
        NOTE: DATA statement used (Total process time):
              real time           2:58.07
              cpu time            2:57.89



        NOTE: There were 4800000 observations read from the data set G.C2FINAL.
              WHERE ryear='2002';
        NOTE: There were 4800000 observations read from the data set G.C2FINAL.
              WHERE ryear='2002';
        NOTE: There were 13600100 observations read from the data set F._2004.
              WHERE year='2002';



        NOTE: SAS Institute Inc., SAS Campus Drive, Cary, NC USA 27513-2414
        NOTE: The SAS System used:
              real time           2:59.82
              cpu time            2:58.29


    *                _               _       _
     _ __ ___   __ _| | _____     __| | __ _| |_ __ _
    | '_ ` _ \ / _` | |/ / _ \   / _` |/ _` | __/ _` |
    | | | | | | (_| |   <  __/  | (_| | (_| | || (_| |
    |_| |_| |_|\__,_|_|\_\___|   \__,_|\__,_|\__\__,_|

    ;

    NOTE I ADDED TWO INDEXES (about 3 minutes);

    libname f "f:/wrk";  /* 68,000,000 */
    data f._2004;
       array as[20] $8 a1-a20 (20*'12345678');
       array ns[20] n1-n20 (20*12345678);

       do year='2001','2002','2003','2004','2005';
          do MSA='A','B','C','D';
             do CountyCode='12','13','14','15','16';
                do State='AK','AL','CA','VT','TX';
                   do Loan_Amount=0 to 27200000 by 200;
                     output;
                   end;
                end;
             end;
          end;
       end;
    run;quit;

    proc sql;
      create index year on f._2004
    ;quit;

    NOTE: Simple index year has been defined.
    560 !  quit;
    NOTE: PROCEDURE SQL used (Total process time):
          real time           2:36.17
          user cpu time       56.09 seconds
          system cpu time     2:21.75
          memory              71939.07k
          OS Memory           85932.00k
          Timestamp           01/29/2018 04:05:35 PM
          Step Count                        67  Switch Count  3

    libname g "g:/wrk"; /* 24,000,000 */
    data g.c2final;
       array as[40] $8 b1-b40 (40*'12345678');
       array ns[28]    m1-m28 (28*12345678);

       do ryear='2001','2002','2003','2004','2005';
          do rMSA='A','B','C','D';
             do rCountyCode='12','13','14','15','16';
                do rState='AK','AL','CA','VT','TX';
                   do rLoan_Amount=1 to 48000 by 1;
                         output;
                   end;
                end;
             end;
          end;
       end;

    run;quit;

    proc sql;
      create index ryear on g.c2final
    ;quit;

    NOTE: PROCEDURE SQL used (Total process time):
          real time           1:25.53
          user cpu time       20.87 seconds
          system cpu time     1:20.04
          memory              71940.42k
          OS Memory           85932.00k
          Timestamp           01/29/2018 04:10:28 PM
          Step Count                        68  Switch Count  4

    *_                 _
    | | ___   ___ __ _| |_ ___  ___
    | |/ _ \ / __/ _` | __/ _ \/ __|
    | | (_) | (_| (_| | ||  __/ (__
    |_|\___/ \___\__,_|\__\___|\___|

    ;

    * could not figure out how to quickly get firtsobs and lastobs for each year.
    * this code will give you a middle record number for each year;
    %macro utl_locatec(datBig,outDsn,var);

        data  not_found &outDsn /* (keep=&var. mid) */;

         do &var.='2001','2002','2003','2004','2005';
               _low    =1;
             _high     =nobs;
             _direction=0;
             _tries    =0;

             do while(_high>_low and &var. ne _target and _tries<(log2(nobs)+2));

               _tries+1;
               if _direction<0 then _high=(_mid-1)<>1;
               else if _direction>0 then _low=(_mid+1)><nobs;

               _mid=int((_low+_high)/2);

                set &datBig(keep=&var. rename=(&var.=_target))
                point=_mid nobs=nobs;
               _direction= sum(((&var.<_target)*(-1)),((&var.>_target)*1));

             end;

             set &datBig(rename=(&var.=_target)) point=_mid;
             mid=_mid;

             if &var.=_target then output &outDsn;
             else output not_found;
         end;
         stop;

        run;quit;

    %mend utl_locatec;

    %utl_locatec(f._2004,recBigDat,year);
    %utl_locatec(g.c2final,recLtlOut,ryear);



