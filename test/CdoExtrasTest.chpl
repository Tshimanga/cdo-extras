use NumSuch,
    CdoExtras,
    Postgres,
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
    var nameTable = "r.cho_names",
        idField = "ftr_id",
        nameField = "name",
        edgeTable = "r.cho_edges",
        fromField = "from_fid",
        toField = "to_fid",
        wField = "w",
        wTable = "r.condition_w",
        n = 8;

    var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);
    var nm = NamedMatrixFromPGRectangular(con=con, edgeTable=edgeTable, fromField=fromField
      , toField=toField);
    // Should have loaded the data from test/reference/entropy_base_graph_schema.sql
    /*
    var vnames = vNamesFromPG(con=con, nameTable=nameTable, nameField=nameField, idField=idField);
    writeln(vnames);
    var X = wFromPG(con=con, edgeTable=edgeTable, fromField=fromField, toField=toField
      , wField=wField, n=vnames.size);
    writeln(X);
    */
    //persistSparseMatrix(con, aTable=wTable, fromField=fromField, toField=toField, weightField=wField, X=X);
  }

  proc run() {
    super.run();
    testPingPostgres();
    testBuildNamedMatrix();

    return 0;
  }
}

proc main(args: [] string) : int {
  var t = new CdoExtrasTest(verbose=false);
  var ret = t.run();
  t.report();
  return ret;
}
