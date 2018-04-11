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
    writeln("Domain of Named Matrix: ",nm.SD.size);
    writeln("Total Loadtime: ",t.elapsed());
    writeln(max(1,3));

    var t1: Timer;
    t1.start();
    var prod = dot(nm.X,nm.X);
    t1.stop();
    writeln("Time to Compute Ben's Dot Product: ",t1.elapsed());

    var idom: domain(1) = {nm.X.domain.dim(1)};
    var is: sparse subdomain(idom);
    var t2: Timer;
    t2.start();
    for (i,k) in nm.X.domain {
      if ! is.member(i) {
        is += i;
      }
    }
    t2.stop();
    writeln("Length of i's: ",is.size);
    writeln("Time to Pick Off i's: ",t2.elapsed());

    var jdom: domain(1) = {nm.X.domain.dim(2)};
    var js: sparse subdomain(jdom);
    var t3: Timer;
    t3.start();
    for (k,j) in nm.X.domain {
      if ! js.member(j) {
        js += j;
      }
    }
    t3.stop();
    writeln("Length of j's: ",js.size);
    writeln("Time to Pick Off j's: ",t3.elapsed());


    var t4: Timer;
    t4.start();
    var I = identityMat(140000: int);
    t4.stop();
    writeln("Time to Generate 140000 by 140000 Id Mat: ",t4.elapsed());
/*

    var t1: Timer;
    t1.start();
    var g = new Graph(nm);
    t1.stop();
    writeln("Time to Create Graph from NamedMatrix: ",t1.elapsed());
    writeln("Graph is directed? ",g.directed);
    writeln("Size of Sparse Domain (should be ~10^6): ",g.X.domain.size);
    writeln("Number of Vertices (should be ~10^5): ",g.verts.size());


    var t2: Timer;
    t2.start();
    g.intoxicate();
    t2.stop();

    writeln("Time to Intoxicate: ",t2.elapsed());
    var s2 = true;
    for (i,j) in g.X.domain {
      s2 &&= (i,j) == (j,i);
    }
    writeln(! s2);



    var t3: Timer;
    t3.start();
    var distM = distMatrix(g);
    t3.stop();

    writeln("Time to Compute all Distances: ", t3.elapsed());
    var diam = max reduce distM;
    writeln("Diameter: ",diam);

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
