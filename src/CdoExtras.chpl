/* Documentation for CdoExtras */
module CdoExtras {
  use Cdo,
      NumSuch;


/*
 Creates a NamedMatrix from a table in Postgres.  Does not optimize for square matrices.  This assumption
 is that the matrix is sparse.

 :arg string edgeTable: The SQL table holding the values of the matrix.
 :arg string fromField: The table column representing rows, e.g. `i`.
 :arg string toField: The table column representing columns, e.g. 'j'.
 :arg string wField: `default=NONE` the table column containing the values of cell `(i,j)``
 :arg boolean square: Whether the matrix should be built to have the same rows and columns
 */
 proc NamedMatrixFromPG(con: Connection
   , edgeTable: string
   , fromField: string, toField: string, wField: string = "NONE"
   , square=false) {
  if square {
    return NamedMatrixFromPGSquare(con: Connection
      , edgeTable, fromField, toField, wField);
  } else {
    return NamedMatrixFromPGRectangular(con: Connection
      , edgeTable, fromField, toField, wField);
  }
}

proc NamedMatrixFromPGRectangular(con: Connection
  , edgeTable: string
  , fromField: string, toField: string, wField: string = "NONE") {

  var q = """
  SELECT ftr, t
  FROM (
    SELECT distinct(%s) AS ftr, 'r' AS t FROM %s
    UNION ALL
    SELECT distinct(%s) AS ftr, 'c' AS t FROM %s
  ) AS a
  GROUP BY ftr, t
  ORDER BY ftr, t ;
  """;

  var rows: BiMap = new BiMap(),
      cols: BiMap = new BiMap();

  var cursor = con.cursor();
  cursor.query(q, (fromField, edgeTable, toField, edgeTable));

  for row in cursor {
    if row['t'] == 'r' {
      rows.add(row['ftr']);
    } else if row['t'] == 'c' {
      cols.add(row['ftr']);
    }
  }


  var D: domain(2) = {1..rows.size(), 1..cols.size()},
      SD = CSRDomain(D),
      X: [SD] real;  // the actual data

  var r = """
  SELECT %s, %s
  FROM %s
  ORDER BY %s, %s ;
  """;
  var cursor2 = con.cursor();
  cursor2.query(r, (fromField, toField, edgeTable, fromField, toField));
  const size = cursor2.rowcount(): int;
  var count = 0: int,
      dom = {1..size},
      indices: [dom] (int, int),
      values: [dom] real;

  // This guy is causing problems.  Exterminiate with extreme prejudice
  //forall row in cursor2 {
  //forall row in cursor2 with (+ reduce count) {
  //forall row in cursor2 with (ref count) {
  for row in cursor2 {
    count += 1;
    indices[count]=(
       rows.get(row[fromField])
      ,cols.get(row[toField])
      );

    /* This is defunct for the moment
    if wField == "NONE" {
      values[count] = 1;
    } else {
      values[count] = row[wField]: real;
    } */
  }

  SD.bulkAdd(indices);
  forall (ij, a) in zip(indices, values) {
    X(ij) = a;
  }

  const nm = new NamedMatrix(X=X);
  nm.rows = rows;
  nm.cols = cols;
  return nm;
}


proc NamedMatrixFromPGSquare(con: Connection
  , edgeTable: string
  , fromField: string, toField: string, wField: string = "NONE") {

  var q = """
  SELECT ftr, t
  FROM (
    SELECT distinct(%s) AS ftr, 'r' AS t FROM %s
    UNION ALL
    SELECT distinct(%s) AS ftr, 'c' AS t FROM %s
  ) AS a
  GROUP BY ftr, t
  ORDER BY ftr, t ;
  """;

  var rows: BiMap = new BiMap(),
      cols: BiMap = new BiMap();

  var cursor = con.cursor();
  cursor.query(q, (fromField, edgeTable, toField, edgeTable));

  for row in cursor {
    if row['t'] == 'r' {
      rows.add(row['ftr']);
    } else if row['t'] == 'c' {
      cols.add(row['ftr']);
    }
  }

  var verts = rows.uni(cols);

  var D: domain(2) = {1..verts.size(), 1..verts.size()},
      SD = CSRDomain(D),
      X: [SD] real;  // the actual data

  var r = """
  SELECT %s, %s
  FROM %s
  ORDER BY %s, %s ;
  """;
  var cursor2 = con.cursor();
  cursor2.query(r, (fromField, toField, edgeTable, fromField, toField));
  const size = cursor2.rowcount(): int;
  var count = 0: int,
      dom = {1..size},
      indices: [dom] (int, int),
      values: [dom] real;

  // This guy is causing problems.  Exterminiate with extreme prejudice
  //forall row in cursor2 {
  //forall row in cursor2 with (+ reduce count) {
  //forall row in cursor2 with (ref count) {
  for row in cursor2 {
    count += 1;
    indices[count]=(
       rows.get(row[fromField])
      ,cols.get(row[toField])
      );

    /* This is defunct for the moment
    if wField == "NONE" {
      values[count] = 1;
    } else {
      values[count] = row[wField]: real;
    } */
  }

  SD.bulkAdd(indices);
  forall (ij, a) in zip(indices, values) {
    X(ij) = a;
  }

  const nm = new NamedMatrix(X=X);
  nm.rows = rows;
  nm.cols = cols;
  return nm;
}


  /*
   Build a square version of the matrix.  Still directed, but with the same number of rows/cols
   */
  proc NamedMatrixFromPGSquare_( con: Connection
      , edgeTable: string
      , fromField: string, toField: string, wField: string = "NONE") {

      var q = """
      SELECT ftr
      FROM (
        SELECT distinct(%s) AS ftr FROM %s
        UNION ALL
        SELECT distinct(%s) AS ftr FROM %s
      ) AS a
      GROUP BY ftr ORDER BY ftr;
      """;

      var cursor = con.cursor();
      cursor.query(q, (fromField, edgeTable, toField, edgeTable));
      var rows: BiMap = new BiMap();

      for row in cursor {
      //for row in cursor {
        rows.add(row['ftr']);
      }

      var D: domain(2) = {1..rows.size(), 1..rows.size()},
          SD = CSRDomain(D),
          X: [SD] real;  // the actual data

      var r = """
      SELECT %s, %s
      FROM %s
      ORDER BY %s, %s ;
      """;
      var cursor2 = con.cursor();
      cursor2.query(r, (fromField, toField, edgeTable, fromField, toField));
      var dom1: domain(1) = {1..0},
          dom2: domain(1) = {1..0},
          indices: [dom1] (int, int),
          values: [dom2] real;
      //forall row in cursor2 {
      for row in cursor2 {
        indices.push_back((
           rows.get(row[fromField])
          ,rows.get(row[toField])
          ));

        if wField == "NONE" {
          values.push_back(1);
        } else {
          values.push_back(row[wField]: real);
        }
      }

      SD.bulkAdd(indices);
      forall (ij, a) in zip(indices, values) {
        X(ij) = a;
      }

      const nm = new NamedMatrix(X=X);
      nm.rows = rows;
      nm.cols = rows;
      return nm;
  }

  proc NamedMatrixFromPG_(con: Connection
    , edgeTable: string
    , fromField: string, toField: string, wField: string = "NONE") {

    var q = """
    SELECT ftr, t, row_number() OVER(PARTITION BY t ORDER BY ftr ) AS ftr_id
    FROM (
      SELECT distinct(%s) AS ftr, 'r' AS t FROM %s
      UNION ALL
      SELECT distinct(%s) AS ftr, 'c' AS t FROM %s
    ) AS a
    GROUP BY ftr, t
    ORDER BY ftr_id, t ;
    """;

    var rows: BiMap = new BiMap(),
        cols: BiMap = new BiMap();

    var cursor = con.cursor();
    cursor.query(q, (fromField, edgeTable, toField, edgeTable));
    for row in cursor {
      if row['t'] == 'r' {
        rows.add(row['ftr'], row['ftr_id']:int);
      } else if row['t'] == 'c' {
        cols.add(row['ftr'], row['ftr_id']:int);
      }
    }

    var D: domain(2) = {1..rows.size(), 1..cols.size()},
        SD = CSRDomain(D),
        X: [SD] real;  // the actual data

    var r = """
    SELECT %s, %s
    FROM %s
    ORDER BY %s, %s ;
    """;
    var cursor2 = con.cursor();
    cursor2.query(r, (fromField, toField, edgeTable, fromField, toField));
    var dom1: domain(1) = {1..0},
        dom2: domain(1) = {1..0},
        indices: [dom1] (int, int),
        values: [dom2] real;

    //forall row in cursor2 {
    for row in cursor2 {
  //    writeln("row: ", row[fromField], " -> ", row[toField]);
      indices.push_back((
         rows.get(row[fromField])
        ,cols.get(row[toField])
        ));

      if wField == "NONE" {
        values.push_back(1);
      } else {
        values.push_back(row[wField]: real);
      }
    }
    SD.bulkAdd(indices);
    for (ij, a) in zip(indices, values) {
      X(ij) = a;
    }

    const nm = new NamedMatrix(X=X);
    nm.rows = rows;
    nm.cols = cols;
    return nm;
  }


  /*

    :arg con: A CDO Connection to Postgres
    :arg edgeTable: The table in PG of edges
    :arg fromField: The field of edgeTable containing the id of the head vertex
    :arg toField: the field of edgeTable containing the id of the tail vertex
    :arg wField: The field of edgeTable containing the weight of the edge
    :arg n: number of distinct vertices. In practice, this may be gives and the number of names
    :arg weights: Boolean on whether to use the weights in the table or a 1 (indicator)
   *//*
  proc wFromPG(con: Connection, edgeTable: string
      , fromField: string, toField: string, wField: string, weights=false) {
    var q = "SELECT %s, %s FROM %s ORDER BY 1, 2;";
    var source_size_q = """
                        SELECT count(*) AS n FROM (SELECT distinct(s.source_cui) FROM r.cui_confabulation s) AS sources;
                        """;

    var target_size_q = """
                        SELECT count(*) AS n FROM (SELECT distinct(s.exhibited_cui) FROM r.cui_confabulation s) AS exhibited;
                        """;
    var cursor2 = con.cursor();
    var cursor3 = con.cursor();
    cursor2.query(source_size_q);
    cursor3.query(target_size_q);
    const row2 = cursor2.fetchone();
    const row3 = cursor3.fetchone();
    const source_size = try row2['n']: int;
    const exhibit_size = try row3['n']: int;

    var cursor = con.cursor();
    cursor.query(q,(fromField, toField, edgeTable));
    const size = cursor.rowcount(): int;
    var D: domain(2) = {1..source_size, 1..exhibit_size};
    var SD: sparse subdomain(D) dmapped CS();
    var X: [SD] real;
    var dom: domain(1) = {1..size};
    var indices: [dom] (int, int);
    var values: [dom] real;
    forall (row, i) in zip(cursor, dom) {
      indices[i] = (row[fromField]: int,row[toField]: int);
      values[i] = 1: real;
    }
    SD.bulkAdd(indices);
    forall (ij, a) in zip(indices, values) {
      if weights {
        X(ij) = a;
      } else {
        X(ij) = 1;
      }
    }
    return X;
  }

  //SERIAL EXTRACTION FUNCTION FOR PERFORMANCE COMPARISONS
  proc wFromPG_(con: Connection, edgeTable: string
      , fromField: string, toField: string, wField: string, n: int, weights=true) {
    var q = "SELECT %s, %s, %s FROM %s ORDER BY 1, 2;";
    var cursor = con.cursor();
    cursor.query(q,(fromField, toField, wField, edgeTable));
    const D: domain(2) = {1..n, 1..n};
    var SD: sparse subdomain(D) dmapped CS();
    var W: [SD] real;

    for row in cursor {
      SD += (row[fromField]: int, row[toField]:int);
      if weights {
        W[row[fromField]:int, row[toField]:int] = row[wField]: real;
      } else {
        W[row[fromField]:int, row[toField]:int] = 1;
      }
    }
     return W;
  }
*/

  /*

   :arg con: A connection to a Postgres database containing a table with <ftr_id>, <vertex_name> pairs
   :arg nameTable: The name of the Postgres table containing the pairs
   :arg nameField: The name of the field in the nameTable containing the names
   :arg idField: The name of the field in the nameTable containing the feature ids

   :returns: An array of strings in order of feature id
   */
  proc vNamesFromPG(con: Connection, nameTable: string
    , nameField: string, idField: string ) {

    var cursor = con.cursor();
    var q1 = "SELECT max(%s) AS n FROM %s";
    cursor.query(q1, (idField, nameTable));
    var n:int= cursor.fetchone()['n']: int;
    var vertexNames: [1..n] string;

    var q2 = "SELECT %s, %s FROM %s ORDER BY 1";
    cursor.query(q2, (idField, nameField, nameTable));
    for row in cursor {
        vertexNames[row[idField]:int ] = row[nameField];
    }
    return vertexNames;
  }

  // BATCH PERSISTENCE
   proc persistSparseMatrix(con: Connection, aTable: string
     , fromField: string, toField: string, weightField: string
     , X:[?D] real) {
     const q = "INSERT INTO %s (%s, %s, %s) VALUES (%s, %s, %s);";
     var cur = con.cursor();
     var count: int = 0;
     var dom: domain(1, int, false) = {1..0};
     var ts: [dom] (string, string, string, string, int, int, real);
     for ij in X.domain {
       ts.push_back((aTable, fromField, toField, weightField, ij(1), ij(2), X(ij)));
       count += 1;
       if count >= batchsize {
         cur.execute(q, ts);
         count = 0;
         var reset: [dom] (string, string, string, string, int, int, real);
         ts = reset;
       }
     }
     cur.execute(q,ts);
   }

  //SERIAL PERSISTANCE FUNCTION FOR PERFORMANCE COMPARISONS
  proc persistSparseMatrix_(con: Connection, aTable: string
    , fromField: string, toField: string, weightField: string
    , X:[?D] real) {
    const q = "INSERT INTO %s (%s, %s, %s) VALUES (%s, %s, %s);";
    var cur = con.cursor();
    for ij in X.domain {
      const d: domain(1) = {1..0};
      var t: [d] (string, string, string, string, int, int, real);
      t.push_back((aTable, fromField, toField, weightField, ij(1), ij(2), X(ij)));
      cur.execute(q, t);
    }
  }


  // PARALLEL PERSISTENCE FUNCTION FOR PERFORMANCE COMPARISONS
  proc persistSparseMatrix_P(con: Connection, aTable: string
    , fromField: string, toField: string, weightField: string
    , X:[?D] real) {
    const q = "INSERT INTO %s (%s, %s, %s) VALUES (%s, %s, %s);";
    var cur = con.cursor();
    forall ij in X.domain {
      const d: domain(1) = {1..0};
      var t: [d] (string, string, string, string, int, int, real);
      t.push_back((aTable, fromField, toField, weightField, ij(1), ij(2), X(ij)));
      cur.execute(q, t);
    }
  }

}
