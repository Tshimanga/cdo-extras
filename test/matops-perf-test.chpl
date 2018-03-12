use NumSuch,
    Postgres,
    Time;

// PERFORMANCE BENCHMARKS FOR MATRIX DB INTERACTIONS

// RESULTS ARE AT THE BOTTOM OF THE FILE IN A MULTILINE COMMENT




config const DB_HOST: string = "localhost";
config const DB_USER: string = "postgres";
config const DB_NAME: string = "matops";
config const DB_PWD: string = "noether";
var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

var nameTable1 = "test1",
    idField = "ftr_id",
    nameField = "name",
    edgeTable1 = "test1",
    fromField = "from_id",
    toField = "to_id",
    wField = "w",
    wTable1 = "test1",
    n = 10000;

var nameTable2 = "test2",
//    idField = "ftr_id",
//    nameField = "name",
    edgeTable2 = "test2",
//    fromField = "from_id",
//    toField = "to_id",
//    wField = "w",
    wTable2 = "test2";
    //    n = 8;

var nameTable3 = "test3",
//    idField = "ftr_id",
//    nameField = "name",
    edgeTable3 = "test3",
    //    fromField = "from_id",
    //    toField = "to_id",
    //    wField = "w",
    wTable3 = "test3";
    //    n = 8;

config param batchsize: int = 1000;


//GENERATING THE MATRIX. THE GENERATOR IS REALLY INEFFICIENT

var t5: Timer;
t5.start();
var W = generateRandomSparseMatrix(n,0.90);
t5.stop();
writeln("  Generation time %n".format(t5.elapsed()));

//ONE COPY OF THE MATRIX GETS PERSISTED TO AN EMPTY TABLE 1
var t1: Timer;
t1.start();
persistSparseMatrix(con, aTable=wTable1, fromField=fromField, toField=toField, weightField=wField, A=W);
t1.stop();
writeln("  Batch Persistence time %n".format(t1.elapsed()));

//SECOND COPY GETS PERSISTED TO AN EMPTY TABLE 2
var t2: Timer;
t2.start();
persistSparseMatrix_(con, aTable=wTable2, fromField=fromField, toField=toField, weightField=wField, A=W);
t2.stop();
writeln("  Regular Persistence time %n".format(t2.elapsed()));

// LAST COPY GETS PERSISTED TO AN EMPTY TABLE 3
var t6: Timer;
t6.start();
persistSparseMatrix_(con, aTable=wTable3, fromField=fromField, toField=toField, weightField=wField, A=W);
t6.stop();
writeln("  Regular Persistence time %n".format(t6.elapsed()));


//INSERTION PERFORMANCE QUOTIENT
const delta1 = t1.elapsed()/t2.elapsed();
const delta3 = t6.elapsed()/t2.elapsed();

writeln("\n");
writeln("  Batch/Serial = %n".format(delta1));
writeln("  Parallel/Serial = %n".format(delta3));
writeln("\n");
writeln("\n");

// PARALLEL EXTRACTION TIME
var t3: Timer;
t3.start();
wFromPG(con=con, edgeTable=edgeTable1, fromField, toField, wField, n);
t3.stop();
writeln("  Parallel Extraction time %n".format(t3.elapsed()));

// SERIAL EXTRACTION TIME
var t4: Timer;
t4.start();
wFromPG_(con=con, edgeTable=edgeTable2, fromField, toField, wField, n);
t4.stop();
writeln("  Serial Extraction time %n".format(t4.elapsed()));

//EXTRACTION PERFORMANCE QUOTIENT
var delta2 = t3.elapsed()/t4.elapsed();

writeln("\n");
writeln("  Parallel/Serial %n".format(delta2));
writeln("\n");
writeln("\n");

// IT APPEARS THAT POSTGRES IS THE BOTTLENECK IN INSERTION PERFORMANCE
// AS THERE ISN'T MUCH OF A DIFFERENCE IN PERFORMANCE REGARDLESS OF
// HOW IT IS DONE.

/*
[Execution output was as follows:]
  Generation time 110.895
  Batch Persistence time 0.622507
  Serial Persistence time 0.572663
  Parallel Persistence time 0.607646


  Batch/Serial = 1.08704
  Parallel/Serial = 1.06109




In these() standalone, creating 1 tasks
task 0 owns 0..997===================
  Parallel Extraction time 0.03788
  Serial Extraction time 0.170498


  Parallel/Serial 0.222173


*/
