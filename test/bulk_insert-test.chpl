use NumSuch,
    Postgres;

config const DB_HOST: string = "";
config const DB_USER: string = "";
config const DB_NAME: string = "";
config const DB_PWD: string = "";

var nameTable = "r.cho_names",
    idField = "ftr_id",
    nameField = "name",
    edgeTable = "r.cho_edges",
    fromField = "from_fid",
    toField = "to_fid",
    wField = "w",
    wTable = "r.condition_w",
    n = 8;

config param batchsize: int = 3;

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

var X = Matrix(
   [1.0,0.0,1.0,0.0],
   [1.0,0.0,1.0,1.0]);


writeln(X);
writeln("Something is a foot");
writeln(X.domain);
writeln(batchsize);
//writeln(bulkInsertFromArray().type)
var vnames = vNamesFromPG(con=con, nameTable=nameTable, nameField=nameField, idField=idField);
writeln(vnames);
var W = wFromPG(con=con, edgeTable=edgeTable, fromField, toField, wField, n=vnames.size);
writeln(W);
//persistSparseMatrix(con, aTable=wTable, fromField=fromField, toField=toField, weightField=wField, A=X);
//bulkInsertMatrix(con, aTable=wTable, fromField=fromField, toField=toField, weightField=wField, A=W);
