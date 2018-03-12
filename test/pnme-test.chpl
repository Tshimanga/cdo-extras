use Time,
    Postgres,
    NumSuch;






config const DB_HOST: string = "";
config const DB_USER: string = "";
config const DB_NAME: string = "";
config const DB_PWD: string = "";

if DB_HOST == "" { var msg =
  """
Cannot find the file 'db_creds.txt'. Please create it in the current directory with the fields

DB_HOST=
DB_USER=
DB_NAME=
DB_PWD=

And DO NOT check it into GitHub. (In fact, Git will try to ignore it.)
  """;
  writeln(msg);
  halt();
}

var t: Timer;


var con = PgConnectionFactory(host=DB_HOST, user=DB_USER, database=DB_NAME, passwd=DB_PWD);

var nameTable = "r.cui_confabulation",
    idField = "ftr_id",
    nameField = "source_str",
    edgeTable = "r.cui_confabulation",
    fromField = "source_cui",
    toField = "exhibited_cui",
    wField = "NONE",
    wTable = "cc_weight",
    n = 8;



t.start();
const X = wFromPG(con, edgeTable, fromField, toField, wField);
t.stop();


writeln("Load Time: %n".format(t.elapsed()));
writeln("Domain Size: %n".format(X.domain.size));
