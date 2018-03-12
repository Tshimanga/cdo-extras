use NumSuch,
    Postgres;

config const DB_HOST: string = "";
config const DB_USER: string = "";
config const DB_NAME: string = "";
config const DB_PWD: string = "";

if DB_HOST == "" {
  var msg = """
Cannot find the file 'db_creds.txt'.  Please create it in the current directory with the fields

DB_HOST=
DB_USER=
DB_NAME=
DB_PWD=

And DO NOT check it into GitHub. (In fact, Git will try to ignore it.)
  """;
  writeln(msg);
  halt();
}

var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

var nameTable = "r.cho_names",
    idField = "ftr_id",
    nameField = "name",
    edgeTable = "r.cho_edges",
    fromField = "from_fid",
    toField = "to_fid",
    wField = "w",
    wTable = "r.condition_w",
    n = 8;

// Should have loaded the data from test/reference/entropy_base_graph_schema.sql
var vnames = vNamesFromPG(con=con, nameTable=nameTable, nameField=nameField, idField=idField);
writeln(vnames);
var X = wFromPG(con=con, edgeTable=edgeTable, fromField=fromField, toField=toField
  , wField=wField, n=vnames.size);
writeln(X);

persistSparseMatrix(con, aTable=wTable, fromField=fromField, toField=toField, weightField=wField, X=X);

/* Old NamedMatrix stuff */

var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);
var nm2 = NamedMatrixFromPG(con, edgeTable="r.cho_named_edges", fromField="from_nm", toField="to_nm");

assert(nm2.nnz() == 10, "nm2.X has ", nm2.nnz(), " entries, expected 10");
assert(nm2.X.shape[1] == 7, "nm2.X has ", nm2.X.shape[1], " rows expected: ", 7);
assert(nm2.X.shape[2] == 7, "nm2.X has ", nm2.X.shape[2], " cols expected: ", 7);
assert(nm2.sparsity() == 0.20408163265306122449, "nm2.sparsity is ", nm2.sparsity(), " expected: 0.204");
for c in nm2.rows.entries() {
  assert(c(2) == nm2.rows.get(c(1)), c(2), " does not equal ", nm2.rows.get(c(1)));
  assert(c(1) == nm2.rows.get(c(2)), c(1), " does not equal ", nm2.rows.get(c(2)));
}

for c in nm2.cols.entries() {
  assert(c(2) == nm2.cols.get(c(1)), c(2), " does not equal ", nm2.cols.get(c(1)));
  assert(c(1) == nm2.cols.get(c(2)), c(1), " does not equal ", nm2.cols.get(c(2)));
}

/* Check edge, then updating the value by name */
assert(nm2.get('star lord', 'gamora') == 1.0, "star lord is not matched to gamora: ", nm2.get('star lord', 'gamora'));
nm2.set("star lord", "gamora", 2.17);
assert(nm2.get('star lord', 'gamora') == 2.17, "star lord to gamora not set to 2.71, instead: ", nm2.get('star lord', 'gamora'));

nm2.set("yondu", "groot", 13.11);
nm2.update('yondu', 'groot', 0.89);
assert(nm2.get("yondu", "groot") == 14.00, "yondu, groot not updated to 14.00, instead: ", nm2.get("yondu", "groot"));

/* Test the square version of the same matrix */
var nm3 = NamedMatrixFromPG(con, edgeTable="r.cho_named_edges"
  , fromField="from_nm", toField="to_nm", square=true);
assert(nm3.nnz() == 10, "nm3.X has ", nm3.nnz(), " entries, expected 10");
assert(nm3.X.shape[1] == 8, "nm3.X has ", nm3.X.shape[1], " rows expected: ", 8);
assert(nm3.X.shape[2] == 8, "nm3.X has ", nm3.X.shape[2], " cols expected: ", 8);
assert(nm3.sparsity() == 0.15625, "nm3.sparsity is ", nm3.sparsity(), " expected: 0.15625");
