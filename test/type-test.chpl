use NumSuch,
    Postgres;







config const DB_HOST: string = "localhost";
config const DB_USER: string = "postgres";
config const DB_NAME: string = "research";
config const DB_PWD: string = "noether";
var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

var nameTable = "cho_names",
    idField = "ftr_id",
    nameField = "name",
    edgeTable = "cho_edges",
    fromField = "from_fid",
    toField = "to_fid",
    wField = "w",
    wTable = "condition_w",
    n = 8;

config param batchsize: int = 3;

var X = Matrix(
   [1.0,0.0,1.0,0.0],
   [1.0,0.0,1.0,1.0]);

//writeln(generateRandomSparseMatrix(10,0.5));

var number = floor((1 - .6666)*100);

writeln(number);
writeln(number.type:string);
writeln(X);
writeln("Something is a foot");
writeln(X.domain);
writeln(batchsize);

writeln(generateRandomSparseMatrix(100,0.8));



var vnames = vNamesFromPG(con=con, nameTable=nameTable, nameField=nameField, idField=idField);
writeln(vnames);
writeln(wFromPG(con=con, edgeTable=edgeTable, fromField, toField, wField, n=vnames.size));
