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
    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);
  }

  proc testBuildNamedMatrix() {
    var edgeTable = "r.cui_confabulation",
        fromField = "source_cui",
        toField = "exhibited_cui";

    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

    var t: Timer;
    t.start();
    var nm = NamedMatrixFromPGSquare(con, edgeTable, fromField, toField, wField = "NONE");
    t.stop();
    writeln("Dimensions: ", nm.D);
    writeln("Number of Nonzeros: ",nm.SD.size);
    writeln("Total Loadtime: ",t.elapsed());

    writeln("");
    writeln("testBuildNamedMatrix... done...");
  }

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


  proc testSibBuilder() {
    // CONNECT TO DB
    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

    var t2: Timer;
    t2.start();
    var siblingMatrix = buildCUIMatrixWithRelType_BATCHED(con, 100000, 'SIB');
    t2.stop();
    writeln("Dimensions of Sibling Matrix: ", siblingMatrix.D);
    writeln("Number of Edges: ",siblingMatrix.SD.size);
    writeln("Total Loadtime: ",t2.elapsed());

    writeln("");
    writeln("testSibBuilder... done...");
  }

  proc testPersistNamedMatrixParallel() {
    //DB CONNECTION
    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);
    //PULL TABLE
    var edgeTable = 'r.cui_confabulation',
        fromField = 'source_cui',
        toField = 'exhibited_cui';

    var t: Timer;
    t.start();
    var nm = NamedMatrixFromPGSquare(con, edgeTable, fromField, toField, wField = "NONE");
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


    /*
    var q = """
    DROP TABLE IF EXISTS %s;
    CREATE TABLE %s (%s int NOT NULL, %s int NOT NULL, %s float NOT NULL);
    """;*/
    var b: Timer;
    b.start();
    var cursor = con.cursor();
    cursor.execute(q,(aTable, aTable, fromField2, toField2, wField));
    b.stop();
    writeln("Time to Prepare Postgres: ",b.elapsed());

    con.close();


    //PERSIST MATRIX
    var pcon = new PgParallelConnection(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);
    var t1: Timer;
    t1.start();
    persistNamedMatrixP(pcon, aTable, fromField2, toField2, wField, nm);
  //  persistSparseMatrix(con, 1000, aTable, fromField2, toField2, wField, nm.X);
    t1.stop();
    writeln("Time to Persist the NamedMatrix: ",t1.elapsed());

    writeln("");
    writeln("testPeristNamedMatrix... done... ");
  }


  proc run() {
    super.run();
    testPersistNamedMatrixParallel();
  //  testPingPostgres();
  //  testBuildNamedMatrix();
  //  testParBuilder();
  //  testSibBuilder();

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
