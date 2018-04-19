use NumSuch,
    Chingon,
    CdoExtras,
    Postgres,
    Time,
    Charcoal;

config const DB_HOST: string = "";
config const DB_USER: string = "";
config const DB_NAME: string = "";
config const DB_PWD: string = "";

class CdoExtrasTest : UnitTest {
  proc init(verbose:bool) {
    super.init(verbose=verbose);
    this.complete();
  }

  proc setUp() { }



  proc testPingPostgres() {

    writeln("testPingPostgres... starting...");
    writeln("");

    var t: Timer;
    t.start();
    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);
    t.stop();
    writeln("Connection Time: ",t.elapsed());

    writeln("");
    writeln("testPingPostgres... done...");
    writeln("");
    writeln("");
  }




//
//
//
//
//
//




proc testBuildNamedMatrix() {
  writeln("testBuildNamedMatrix... starting...");
  writeln("");

  var edgeTable = "r.cui_confabulation",
      fromField = "source_cui",
      toField = "exhibited_cui";

  var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

  var t: Timer;
  t.start();
  // NamedMatrixFromPGSquare_ is the verbose version of the procedure
  var nm = NamedMatrixFromPGSquare_(con, edgeTable, fromField, toField, wField = "NONE");
  t.stop();
  writeln("Dimensions: ", nm.D);
  writeln("Number of Nonzeros: ",nm.SD.size);
  writeln("Total Loadtime: ",t.elapsed());

  con.close();

  writeln("");
  writeln("testBuildNamedMatrix... done...");
  writeln("");
  writeln("");
}




//
//
//
//
//
//




proc testBatchedExtractor() {
  writeln("testBatchedExtractor... starting...");
  writeln("");

  var edgeTable = "r.cui_confabulation",
      fromField = "source_cui",
      toField = "exhibited_cui";

  var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

  var t: Timer;
  t.start();
  // NamedMatrixFromPGSquare_ is the verbose version of the procedure
  var nm = NMFromPGSquare_BATCHED(con, 100000, edgeTable, fromField, toField);
  t.stop();
  writeln("Dimensions: ", nm.D);
  writeln("Number of Nonzeros: ",nm.SD.size);
  writeln("Total Loadtime: ",t.elapsed());

  con.close();

  writeln("");
  writeln("testBatchedExtractor... done...");
  writeln("");
  writeln("");
}




//
//
//
//
//
//




  proc testPersistNamedMatrix() {
    writeln("testingPersistNamedMatrix... starting...");
    writeln("");
    //DB CONNECTION
    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);
    //PULL TABLE
    var edgeTable = 'r.cui_confabulation',
        fromField = 'source_cui',
        toField = 'exhibited_cui';

    var t: Timer;
    t.start();
    var nm = NamedMatrixFromPGSquare_(con, edgeTable, fromField, toField, wField = "NONE");
    t.stop();
    writeln("Dimensions: ", nm.D);
    writeln("Number of Nonzeros: ",nm.SD.size);
    writeln("Total Loadtime: ",t.elapsed());

    //PREPARE POSTGRES
    var aTable = 'r.cui_confabulation_copy',
        fromField2 = 'source_cui',
        toField2 = 'exhibited_cui',
        wField = 'w';

    var q = """
    DROP TABLE IF EXISTS %s;
    CREATE TABLE %s (%s varchar NOT NULL, %s varchar NOT NULL, %s float NOT NULL);
    """;

    var b: Timer;
    b.start();
    var cursor = con.cursor();
    cursor.execute(q,(aTable, aTable, fromField2, toField2, wField));
    b.stop();
    writeln("Time to Prepare Postgres: ",b.elapsed());

    //PERSIST MATRIX
    var t1: Timer;
    t1.start();
    persistNamedMatrixP(con, aTable, fromField2, toField2, wField, nm);
  //  persistSparseMatrix(con, 1000, aTable, fromField2, toField2, wField, nm.X);
    t1.stop();
    writeln("Time to Persist the NamedMatrix: ",t1.elapsed());

    writeln("");
    writeln("testPeristNamedMatrix... done... ");
    writeln("");
    writeln("");
  }




//
//
//
//
//
//




  proc testParallelPersistence() {
    writeln("testingParallelPersistence... starting...");
    writeln("");

    //DB CONNECTION
    var c: Timer;
    c.start();
    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);
    c.stop();
    writeln("Time to Establish Connection: ",c.elapsed());
    //PULL TABLE
    var edgeTable = 'r.cui_confabulation',
        fromField = 'source_cui',
        toField = 'exhibited_cui';

    var t: Timer;
    t.start();
    var nm = NamedMatrixFromPGSquare_(con, edgeTable, fromField, toField, wField = "NONE");
    t.stop();
    writeln("Dimensions: ", nm.D);
    writeln("Number of Nonzeros: ",nm.SD.size);
    writeln("Total Loadtime: ",t.elapsed());

    con.close();

    //PREPARE POSTGRES
    var aTable = 'r.cui_confabulation_copy',
        fromField2 = 'source_cui',
        toField2 = 'exhibited_cui',
        wField = 'w';

    //DB CONNECTION
    var c2: Timer;
    c2.start();
    var con2 = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);
    c2.stop();
    writeln("Time to Establish New Connection: ",c2.elapsed());

    var q = """
    DROP TABLE IF EXISTS %s;
    CREATE TABLE %s (%s varchar NOT NULL, %s varchar NOT NULL, %s float NOT NULL);
    """;

    var b: Timer;
    b.start();
    var cursor2 = con2.cursor();
    cursor2.execute(q,(aTable, aTable, fromField2, toField2, wField));
    b.stop();
    writeln("Time to Prepare Postgres: ",b.elapsed());

    con2.close();

    var pcon = new PgParallelConnection(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

    //PERSIST MATRIX
    var t1: Timer;
    t1.start();
    persistNMParallelConn(pcon, aTable, fromField2, toField2, wField, nm);
  //  persistSparseMatrix(con, 1000, aTable, fromField2, toField2, wField, nm.X);
    t1.stop();
    writeln("Time to Persist Using Concurrent Connections: ",t1.elapsed());

    writeln("");
    writeln("testParallelPersistence... done... ");
    writeln("");
    writeln("");
  }




//
//
//
//
//
//




  proc testParBuilder() {
    // CONNECT TO DB
    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

    //LOADING PARENT MATRIX
    var t1: Timer;
    t1.start();
    var parentMatrix = buildCUIMatrixWithRelType_(con, 'PAR');
    t1.stop();
    writeln("Dimensions of Parent Matrix: ", parentMatrix.D);
    writeln("Number of Edges: ",parentMatrix.SD.size);
    writeln("Total Loadtime: ",t1.elapsed());
/*
    var aTable = 'a.umls_par_rel',
        fromField = 'cui1',
        toField = 'cui2',
        wField = 'w';

    //PREPARING POSTGRES
    var q = """
    DROP TABLE IF EXISTS %s;
    CREATE TABLE %s (%s varchar NOT NULL, %s varchar NOT NULL, %s float NOT NULL);
    """;
    var b: Timer;
    b.start();
    var cursor = con.cursor();
    cursor.execute(q,(aTable, aTable, fromField, toField, wField));
    b.stop();
    writeln("Time to Prepare Postgres: ",b.elapsed());

    //PERSISTING PARENT MATRIX
    var t2: Timer;
    t2.start();
    persistNamedMatrixP(con, aTable, fromField, toField, wField, parentMatrix);
  //  persistSparseMatrix(con, 1000, aTable, fromField2, toField2, wField, parentMatrix.X);
    t2.stop();
    writeln("Time to Persist PAR: ",t2.elapsed());
*/
    writeln("");
    writeln("testParBuilder... done...");
  }




//
//
//
//
//
//




  proc testParOps() {
    // CONNECT TO DB
    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

    // LOADING PARENT MATRIX
    var edgeTable = 'a.umls_par_rel',
        fromField = 'cui1',
        toField = 'cui2',
        wField = 'w';

    var t1: Timer;
    t1.start();
    var parentMatrix = NamedMatrixFromPGSquare(con, edgeTable, fromField, toField, wField);
    t1.stop();
    writeln("Dimensions of Parent Matrix: ", parentMatrix.D);
    writeln("Number of Edges: ",parentMatrix.SD.size);
    writeln("Total Loadtime: ",t1.elapsed());

/*
    var parTrans = parentMatrix.transpose();
    var simSquare = parentMatrix.ndot(parTrans);




    var aTable = 'a.umls_par_rel',
        fromField = 'cui1',
        toField = 'cui2',
        wField = 'w';

    //PREPARING POSTGRES
    var q = """
    DROP TABLE IF EXISTS %s;
    CREATE TABLE %s (%s varchar NOT NULL, %s varchar NOT NULL, %s float NOT NULL);
    """;
    var b: Timer;
    b.start();
    var cursor = con.cursor();
    cursor.execute(q,(aTable, aTable, fromField, toField, wField));
    b.stop();
    writeln("Time to Prepare Postgres: ",b.elapsed());

    //PERSISTING PARENT MATRIX
    var t2: Timer;
    t2.start();
    persistNamedMatrix(con, aTable, fromField, toField, wField, parentMatrix);
    t2.stop();
    writeln("Time to Persist PAR: ",t2.elapsed());
*/
    writeln("");
    writeln("testParBuilder... done...");
  }




//
//
//
//
//
//




  proc testSibBuilder() {
    // CONNECT TO DB
    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

    var t2: Timer;
    t2.start();
    var siblingMatrix = buildCUIMatrixWithRelType(con, 'SIB');
    t2.stop();
    writeln("Dimensions of Sibling Matrix: ", siblingMatrix.D);
    writeln("Number of Edges: ",siblingMatrix.SD.size);
    writeln("Total Loadtime: ",t2.elapsed());

    writeln("");
    writeln("testSibBuilder... done...");
  }




//
//
//
//
//
//




  proc run() {
    super.run();
  //  testPingPostgres();
  //  testBuildNamedMatrix();
    testBatchedExtractor();
  //  testParallelPersistence();
  //  testPersistNamedMatrix();
  //  testParBuilder();
  //  testSibBuilder();
  //  testParOps();

    return 0;
  }
}


proc main(args: [] string) : int {
  const msg = """
Please make sure your test database has been populated by the SQL script in
  test/reference/entropy_base_graph_schema.sql
""";
  writeln(msg);
  var t = new CdoExtrasTest(verbose=false);
  var ret = t.run();
  t.report();
  return ret;
}
