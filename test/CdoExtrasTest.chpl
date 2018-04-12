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

    writeln("testBuildNamedMatrix... done...");
  }

  proc testPersistNamedMatrix() {
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

    //PERSIST MATRIX
    var t1: Timer;
    t1.start();
    persistNamedMatrixP(con, aTable, fromField2, toField2, wField, nm);
  //  persistSparseMatrix(con, 1000, aTable, fromField2, toField2, wField, nm.X);
    t1.stop();
    writeln("Time to Persist the NamedMatrix: ",t1.elapsed());

    writeln("testPeristNamedMatrix... done... ");
  }


  proc run() {
    super.run();
    testPersistNamedMatrix();
  //  testPingPostgres();
  //  testBuildNamedMatrix();

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
