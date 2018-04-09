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
    var t: Timer;
    t.start();
    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);
    t.stop();
    writeln("Time to Establish Connection to the DB: ",t.elapsed());

    var t1: Timer;
    t1.start();
    var cursor = con.cursor();
    t1.stop();
    writeln("Time to Create Cursor: ",t1.elapsed());

    var q = """
    SELECT ftr
    FROM (
      SELECT distinct(source_cui) AS ftr FROM r.cui_confabulation
      UNION ALL
      SELECT distinct(exhibited_cui) AS ftr FROM r.cui_confabulation
    ) AS a
    GROUP BY ftr ORDER BY ftr;
    """;

    var t2: Timer;
    t2.start();
    var rows: BiMap = new BiMap();
    t2.stop();
    writeln("Time to Initialize Rows BiMap: ",t2.elapsed());

    var t3: Timer;
    t3.start();
    cursor.query(q);
    t3.stop();
    writeln("Time to Query the DB: ",t3.elapsed());

    var t4: Timer;
    t4.start();
    for row in cursor {
      rows.add(row['ftr']);
    }
    t4.stop();
    writeln("Time to Populate Rows BiMap: ",t4.elapsed());

    var t5: Timer;
    t5.start();
    var size = rows.size();
    t5.stop();
    writeln("Time to Measure Length of Row BiMap: ",t5.elapsed());

    var t6: Timer;
    t6.start();
    var D: domain(2) = {1..rows.size(), 1..rows.size()},
        SD = CSRDomain(D),
        X: [SD] real;
    t6.stop();
    writeln("Time to Initialize Base Matrix: ",t6.elapsed());

    var r = """
    SELECT source_cui, exhibited_cui
    FROM r.cui_confabulation
    ORDER BY source_cui, exhibited_cui ;
    """;


    var cursor2 = con.cursor();
    var t7: Timer;
    t7.start();
    cursor2.query(r);
    t7.stop();
    writeln("Time for Second DB Query: ",t7.elapsed());




    /*
    var t1: Timer;
    t1.start();
    var nm = NamedMatrixFromPGSquare(con=con, edgeTable=edgeTable, fromField=fromField
      , toField=toField);
    t1.stop();

    writeln("Dimensions are: ",nm.D);
    writeln("Time Elapsed to Load Matrix: ",t1.elapsed());

    var g = new Graph(nm);

    var t2: Timer;

    t2.start();
    g.intoxicate();
    t2.stop();

    writeln("Time to Intoxicate: ",t2.elapsed());

    var t3: Timer;
    t3.start();
    var distM = distMatrix(g);
    t3.stop();

    writeln("Time to Compute all Distances: ", t3.elapsed());

    var t4: Timer;
    t4.start();
    var diameter = aMax(distM, axis = 0);
    t4.stop();

    writeln("Diameter: ", diameter);
    writeln("Time to Extract Diameter: ",t4.elapsed());

    var t5: Timer;
    t5.start();
    var comps = components(g);
    t5.stop();

    var numComps = max reduce comps;
    writeln("Number of Components: ", numComps);
    writeln("Time to Calculate the Number of Components: ", t5.elapsed());*/
  //  assertIntEquals("nm has 7 rows", expected=7, actual=nm.nrows());
    // Should have loaded the data from test/reference/entropy_base_graph_schema.sql
    //var vnames = vNamesFromPG(con=con, nameTable=nameTable, nameField=nameField, idField=idField);
    /*
    writeln(vnames);
    var X = wFromPG(con=con, edgeTable=edgeTable, fromField=fromField, toField=toField
      , wField=wField, n=vnames.size);
    writeln(X);
    */
    //persistSparseMatrix(con, aTable=wTable, fromField=fromField, toField=toField, weightField=wField, X=X);
  }


  proc run() {
    super.run();
  //  testPingPostgres();
    testBuildNamedMatrix();

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
