/* Documentation for CdoExtras */
module CdoExtras {
  use Cdo,
      Time,
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

/*
SELECT ftr, t
FROM (
  SELECT distinct(source_cui) AS ftr, 'r' AS t FROM r.cui_confabulation
  UNION ALL
  SELECT distinct(exhibited_cui) AS ftr, 'c' AS t FROM r.cui_confabulation
) AS a
GROUP BY ftr, t
ORDER BY ftr, t ;
*/


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

  var rows1: BiMap = new BiMap(),
      cols1: BiMap = new BiMap();

  var cursor = con.cursor();
  cursor.query(q, (fromField, edgeTable, toField, edgeTable));

  for row in cursor {
    if row['t'] == 'r' {
      rows1.add(row['ftr']);
    } else if row['t'] == 'c' {
      cols1.add(row['ftr']);
    }
  }

  var D: domain(2) = {1..rows1.size(), 1..cols1.size()},
      SD = CSRDomain(D),
      X: [SD] real;  // the actual data
  var nm = new NamedMatrix(X=X);
  nm.rows = rows1;
  nm.cols = cols1;

  var cursor2 = con.cursor();
  if wField == "NONE" {
    var r = """
    SELECT %s, %s
    FROM %s
    ORDER BY %s, %s ;
    """;
    cursor2.query(r, (fromField, toField, edgeTable, fromField, toField));
  } else if wField != "NONE" {
    var r = """
    SELECT %s, %s, %s
    FROM %s
    ORDER BY %s, %s ;
    """;
    cursor2.query(r, (fromField, toField, wField, edgeTable, fromField, toField));
  }

  var size = cursor2.rowcount(): int;
  var count = 0: int,
      dom = {1..size},
      indices: [dom] (int, int),
      values: [dom] real;

  for row in cursor2 {
    count += 1;
    indices[count]=(
       rows1.get(row[fromField])
      ,cols1.get(row[toField])
      );
    if wField != "NONE" {
      try! values[count] = row[wField]: real; // don't understand why the try! is necessary
    }
  }

  nm.SD.bulkAdd(indices);

  if wField == "NONE" {
    for (i,j) in indices {
        nm.X(i,j) = 1;
    }
  } else if wField != "NONE" {
    for (ij, a) in zip(indices, values) {
      nm.X(ij) = a;
    }
  }

  return nm;
}



proc NamedMatrixFromPGSquare(con: Connection
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

    var rows1: BiMap = new BiMap();
    for row in cursor {
      rows1.add(row['ftr']);
    }

    var D: domain(2) = {1..rows1.size(), 1..rows1.size()},
        SD = CSRDomain(D),
        X: [SD] real;
    var nm = new NamedMatrix(X=X);
    nm.rows = rows1;
    nm.cols = rows1;

    var cursor2 = con.cursor();
    if wField == "NONE" {
      var r = """
      SELECT %s, %s
      FROM %s
      ORDER BY %s, %s ;
      """;
      cursor2.query(r, (fromField, toField, edgeTable, fromField, toField));
    } else if wField != "NONE" {
      var r = """
      SELECT %s, %s, %s
      FROM %s
      ORDER BY %s, %s ;
      """;
      cursor2.query(r, (fromField, toField, wField, edgeTable, fromField, toField));
    }

    var size = cursor2.rowcount(): int;
    var count = 0: int,
        dom = {1..size},
        indices: [dom] (int, int),
        values: [dom] real;

    for row in cursor2 {
      count += 1;
      indices[count]=(
         rows1.get(row[fromField])
        ,rows1.get(row[toField])
        );
      if wField != "NONE" {
        try! values[count] = row[wField]: real; // don't understand why the try! is necessary
      }
    }

    nm.SD.bulkAdd(indices);

    if wField == "NONE" {
      forall (i,j) in indices {
          nm.X(i,j) = 1;
      }
    } else if wField != "NONE" {
      forall (ij, a) in zip(indices, values) {
        nm.X(ij) = a;
      }
    }

  return nm;
}




proc NamedMatrixFromPGSquare_(con: Connection
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

    var t: Timer;
    t.start();
    var cursor = con.cursor();
    cursor.query(q, (fromField, edgeTable, toField, edgeTable));
    t.stop();
    writeln("BiMap Query Time: ",t.elapsed());

    var t1: Timer;
    t1.start();
    var rows1: BiMap = new BiMap();
    for row in cursor {
      rows1.add(row['ftr']);
    }
    t1.stop();
    writeln("BiMap Build Time: ",t1.elapsed());
    writeln("BiMap Size: ",rows1.size());

    delete cursor;

    var t2: Timer;
    t2.start();
    var D: domain(2) = {1..rows1.size(), 1..rows1.size()},
        SD = CSRDomain(D),
        X: [SD] real;
    var nm = new NamedMatrix(X=X);
    nm.rows = rows1;
    nm.cols = rows1;
    t2.stop();
    writeln("Time to Initialize NamedMatrix: ",t2.elapsed());


    var t3: Timer;
    t3.start();
    var cursor2 = con.cursor();
    if wField == "NONE" {
      var r = """
      SELECT %s, %s
      FROM %s
      ORDER BY %s, %s ;
      """;
      cursor2.query(r, (fromField, toField, edgeTable, fromField, toField));
    } else if wField != "NONE" {
      var r = """
      SELECT %s, %s, %s
      FROM %s
      ORDER BY %s, %s ;
      """;
      cursor2.query(r, (fromField, toField, wField, edgeTable, fromField, toField));
    }
    t3.stop();
    writeln("Time for Non-zeros Query: ",t3.elapsed());


    var t4: Timer;
    t4.start();
    var size = cursor2.rowcount(): int;
    var count = 0: int,
        dom = {1..size},
        indices: [dom] (int, int),
        values: [dom] real;
    t4.stop();
    writeln("Time to Initialize Index/Value Arrays: ",t4.elapsed());


    var t5: Timer;
    t5.start();
    for row in cursor2 {
      count += 1;
      indices[count]=(
         rows1.get(row[fromField])
        ,rows1.get(row[toField])
        );
      if wField != "NONE" {
        try! values[count] = row[wField]: real; // don't understand why the try! is necessary
      }
    }
    t5.stop();
    writeln("Time to Graft Indices/Values: ",t5.elapsed());
    delete cursor2;

    var t6: Timer;
    t6.start();
    nm.SD.bulkAdd(indices);
    t6.stop();
    writeln("Time to bulkAdd to Sparse Domain: ",t6.elapsed());


    var t7: Timer;
    t7.start();
    if wField == "NONE" {
      forall (i,j) in indices {
          nm.X(i,j) = 1;
      }
    } else if wField != "NONE" {
      forall (ij, a) in zip(indices, values) {
        nm.X(ij) = a;
      }
    }
    t7.stop();
    writeln("Time to Graft Values: ",t7.elapsed());

  return nm;
}







proc buildCUIMatrixWithRelType(con: Connection, relType: string) {
  var q = """
  SELECT ftr
  FROM (
    SELECT distinct(cui1) AS ftr FROM a.umls_parsib_rel
    UNION ALL
    SELECT distinct(cui2) AS ftr FROM a.umls_parsib_rel
  ) AS a
  GROUP BY ftr ORDER BY ftr;
  """; // MAKING SURE THE DOMAIN COVERS ALL CUIs
  var t1: Timer;
  t1.start();
  var vertexCursor = con.cursor();
  vertexCursor.query(q);
  t1.stop();
  writeln("Time to Pull Vertex Data: ", t1.elapsed());


  var t2: Timer;
  t2.start();
  var vertices: BiMap = new BiMap;
  for row in vertexCursor {
    vertices.add(row['ftr']);
  }
  t2.stop();
  delete vertexCursor;
  writeln("Time to Build Vertex Set: ",t2.elapsed());

  var t3: Timer;
  t3.start();
  var D: domain(2) = {1..vertices.size(),1..vertices.size()},
      SD = CSRDomain(D),
      X: [SD] real;
  var nm = new NamedMatrix(X=X);
  nm.rows = vertices;
  nm.cols = vertices;
  t3.stop();
  writeln("Time to Prepare Named Matrix: ",t3.elapsed());

  var r = """
  SELECT cui1, cui2
  FROM (SELECT * FROM a.umls_parsib_rel s WHERE s.rel='%s') AS edges
  ORDER BY cui1, cui2;
  """;

  var edgeCursor = con.cursor();

  var t4: Timer;
  t4.start();
  try! edgeCursor.query(r.format(relType)); // Pull Edges with relType
  t4.stop();
  writeln("Time to Pull Edge Data: ",t4.elapsed());

  var t5: Timer;
  t5.start();
  var size = edgeCursor.rowcount(): int;
  var count = 0: int,
      dom = {1..size},
      indices: [dom] (int, int);
  t5.stop();
  writeln("Time to Initialize Index Buffer: ",t5.elapsed());


  var t6: Timer;
  t6.start();
  for edge in edgeCursor {
    count += 1;
    indices[count]=(
      vertices.get(edge['cui1'])
     ,vertices.get(edge['cui2'])
     );
  }
  t6.stop();
  delete edgeCursor;
  writeln("Time to Graft Indices: ",t6.elapsed());


  var t7: Timer;
  t7.start();
  nm.SD.bulkAdd(indices);  // Expand Sparse Domain in Bulk
  t7.stop();
  writeln("Time to bulkAdd to Sparse Domain: ",t7.elapsed());

  var t8: Timer;
  t8.start();
  for (i,j) in indices {  // Populate Entries
    nm.X(i,j) = 1;
  }
  t8.stop();
  writeln("Time to Write to Array: ",t8.elapsed());

  return nm;
}

proc prepareNamedBase(con: Connection, q: string) {
  var t1: Timer;
  t1.start();
  var vertexCursor = con.cursor();
  vertexCursor.query(q);
  t1.stop();
  writeln("Time to Pull Vertex Data: ", t1.elapsed());


  var t2: Timer;
  t2.start();
  var vertices: BiMap = new BiMap;
  for row in vertexCursor {
    vertices.add(row['node']);
  }
  t2.stop();
  writeln("Time to Build Vertex Set: ",t2.elapsed());

  var t3: Timer;
  t3.start();
  var size: int = vertices.size();
  var D: domain(2) = {1..size,1..size},
      SD = CSRDomain(D),
      X: [SD] real;
  var nm = new NamedMatrix(X=X);
  nm.rows = vertices;
  nm.cols = vertices;
  delete vertexCursor;
  delete vertices;
  t3.stop();
  writeln("Time to Prepare Named Matrix: ",t3.elapsed());

  return nm;
}


proc NamedMatrix.expandSparseDomain(con: Connection, q: string) {
  var edgeCursor = con.cursor();

  var t4: Timer;
  t4.start();
  try! edgeCursor.query(q); // Pull Edges with relType
  t4.stop();
  writeln("Time to Pull Edge Data: ",t4.elapsed());

  var t5: Timer;
  t5.start();
  var size = edgeCursor.rowcount(): int;
  var count = 0: int,
      dom = {1..size},
      indices: [dom] (int, int);
  t5.stop();
  writeln("Time to Initialize Index Buffer: ",t5.elapsed());


  var t6: Timer;
  t6.start();
  for edge in edgeCursor {
    count += 1;
    indices[count]=(
      this.rows.get(edge['cui1'])
     ,this.cols.get(edge['cui2'])
     );
  }
  t6.stop();
  writeln("Time to Graft Indices: ",t6.elapsed());

  delete edgeCursor;

  var t7: Timer;
  t7.start();
  this.SD.bulkAdd(indices);  // Expand Sparse Domain in Bulk
  t7.stop();
  writeln("Time to bulkAdd to Sparse Domain: ",t7.elapsed());
}

proc buildCUIMatrixWithRelType_(con: Connection, relType: string) {
  // MAKING SURE THE DOMAIN COVERS ALL CUIs
  var q = """
  SELECT node
  FROM (
    SELECT distinct(cui1) AS node FROM a.umls_parsib_rel
    UNION ALL
    SELECT distinct(cui2) AS node FROM a.umls_parsib_rel
  ) AS a
  GROUP BY node ORDER BY node;
  """;

  // PREPARE NAMEDMATRIX
  var namedMatrix = prepareNamedBase(con: Connection, q);

  // CHOOSE THE RELATIONSHIP TYPE (sibling or parent)
  var r = """
  SELECT cui1, cui2
  FROM (SELECT * FROM a.umls_parsib_rel s WHERE s.rel='%s') AS edges
  ORDER BY cui1, cui2;
  """;
  try! r.format(relType);

  // EXPAND NAMEDMATRIX EDGE SET/SPARSE DOMAIN
  namedMatrix.expandSparseDomain(con: Connection, r);

  // POPULATE ENTRIES IN EDGE SET
  var t: Timer;
  t.start();
  forall (i,j) in namedMatrix.SD {
    namedMatrix.X(i,j) = 1;
  }
  t.stop();

  // RETURN FINISHED MATRIX
  return namedMatrix;
}

proc buildCUIMatrixWithRelType_BATCHED(con: Connection, batchsize: int, relType: string) {
  // MAKING SURE THE DOMAIN COVERS ALL CUIs
  var q = """
  SELECT node
  FROM (
    SELECT distinct(cui1) AS node FROM a.umls_parsib_rel
    UNION ALL
    SELECT distinct(cui2) AS node FROM a.umls_parsib_rel
  ) AS a
  GROUP BY node ORDER BY node;
  """;

  // PREPARE NAMEDMATRIX
  var namedMatrix = prepareNamedBase(con: Connection, q);

  // BATCHING DETAILS
  var qNumRows = """
  SELECT *
  FROM a.umls_parsib_rel s
  WHERE s.rel='%s';
  """;
  try! qNumRows.format(relType);
  var numRowCursor = con.cursor();
  numRowCursor.query(q);
  var numRows = numRowCursor.rowcount();
  delete numRowCursor;
  var batches = ((numRows/batchsize) + 1): int;
  var count = 0: int;

  for n in {1..batches} {
    var r = """
    SELECT cui1, cui2
    FROM (
      SELECT * FROM a.umls_parsib_rel s WHERE s.rel='%s'
      ) AS edges
    ORDER BY cui1, cui2
    LIMIT %s
    OFFSET %s;
    """;
    var offset = count*batchsize: int;
//    var params: (string, int, int) = (relType, batchsize, count*batchsize);
//    try! r.format(relType, batchsize, offset);
    try{
      r.format(relType, batchsize, offset);
      }catch(e:Error) {
         writeln(e);
      }

    namedMatrix.expandSparseDomain(con: Connection, r);
    count += 1;
  }

  // POPULATE ENTRIES IN EDGE SET
  forall (i,j) in namedMatrix.SD {
    namedMatrix.X(i,j) = 1;
  }

  // RETURN FINISHED MATRIX
  return namedMatrix;
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


   proc persistNamedMatrix(con: Connection, batchsize: int, aTable: string, fromField: string, toField: string, wField: string, N: NamedMatrix) {
     var q: string;
     if wField == "NONE" {
       q = "INSERT INTO %s (%s, %s) VALUES ('%s', '%s');";
       var cur = con.cursor();
       var count: int = 0;
       var dom: domain(1) = {1..0};
       var ts: [dom] (string, string, string, string, string);
       for (i,j) in N.SD {
         var t: Timer;
         t.start();
         ts.push_back((aTable: string, fromField: string, toField: string, N.rows.get(i): string, N.cols.get(j): string));
         count += 1;
         if count >= batchsize {
           cur.execute(q,ts);
           count = 0;
           var reset: [dom] (string, string, string, string, string);
           ts = reset;
           t.stop();
           writeln("Batch Time: ",t.elapsed());
         }
       }
       cur.execute(q,ts);
     } else {
       q = "INSERT INTO %s (%s, %s, %s) VALUES ('%s', '%s', %s);";
       var cur = con.cursor();
       var count: int = 0;
       var dom: domain(1) = {1..0};
       var ts: [dom] (string, string, string, string, string, string, real);
       for (i,j) in N.SD {
         var t: Timer;
         var t1: Timer;
         t.start();
         t1.start();
         ts.push_back((aTable: string, fromField: string, toField: string, wField: string, N.rows.get(i): string, N.cols.get(j): string, N.get(i,j): real));
         count += 1;
         t1.stop();
         writeln("Time to Push Back on Buffer: ",t1.elapsed());
         if count >= batchsize {
           var t2: Timer;
           t2.start();
           cur.execute(q,ts);
           count = 0;
           var reset: [dom] (string, string, string, string, string, string, real);
           ts = reset;
           t2.stop();
           t.stop();
           writeln("Batch Execution Time: ",t2.elapsed());
           writeln("Batch Time Total: ",t.elapsed());
         }
       }
       cur.execute(q,ts);
     }
   }




   proc persistNamedMatrixPB(pcon, batchsize: int, aTable: string, fromField: string, toField: string, wField: string, N: NamedMatrix) {
     var q: string;
     if wField == "NONE" {
       q = "INSERT INTO %s (%s, %s) VALUES ('%s', '%s');";
  //     var cur = con.cursor();
       var count: int = 0;
       var dom: domain(1) = {1..0};
       var ts: [dom] (string, string, string, string, string);
       for (i,j) in N.SD {
//         var t: Timer;
  //       t.start();
         ts.push_back((aTable: string, fromField: string, toField: string, N.rows.get(i): string, N.cols.get(j): string));
         count += 1;
         if count >= batchsize {
  //         pcon.execute(q,ts);
           count = 0;
           var reset: [dom] (string, string, string, string, string);
           ts = reset;
//           t.stop();
//           writeln("Batch Time: ",t.elapsed());
         }
       }
       pcon.execute(q,ts);
     } else {
       q = "INSERT INTO %s (%s, %s, %s) VALUES ('%s', '%s', %s);";
//       var cur = con.cursor();
       var count: int = 0;
       var dom: domain(1) = {1..0};
       var ts: [dom] (string, string, string, string, string, string, real);
       for (i,j) in N.SD {
//         var t: Timer;
//         var t1: Timer;
//         t.start();
//         t1.start();
         ts.push_back((aTable: string, fromField: string, toField: string, wField: string, N.rows.get(i): string, N.cols.get(j): string, N.get(i,j): real));
         count += 1;
//         t1.stop();
  //       writeln("Time to Push Back on Buffer: ",t1.elapsed());
         if count >= batchsize {
//           var t2: Timer;
//           t2.start();
           pcon.execute(q,ts);
           count = 0;
           var reset: [dom] (string, string, string, string, string, string, real);
           ts = reset;
//           t2.stop();
//           t.stop();
//           writeln("Batch Execution Time: ",t2.elapsed());
//           writeln("Batch Time Total: ",t.elapsed());
         }
       }
       pcon.execute(q,ts);
     }
   }


   proc persistNamedMatrixP(pcon, aTable: string
     , fromField: string, toField: string, wField: string
     , N: NamedMatrix) {
     var q: string;
     if wField == "NONE" {
       q = "INSERT INTO %s (%s, %s) VALUES ('%s', '%s');";
//       var cur = con.cursor();
       forall (i,j) in N.SD {
         var d: domain(1) = {1..0};
         var t: [d] (string, string, string, string, string);
         t.push_back((aTable: string, fromField: string, toField: string, N.rows.get(i): string, N.cols.get(j): string));
         pcon.execute(q, t);
      }
     } else {
       q = "INSERT INTO %s (%s, %s, %s) VALUES ('%s', '%s', %s);";
  //     var cur = con.cursor();
       forall (i,j) in N.SD {/*
         var t1: Timer;
         var t2: Timer;
         var t3: Timer;
         var t4: Timer;
         var t5: Timer;
         t1.start();
         t2.start();*/
         var d: domain(1) = {1..0};
//         t2.stop();
//         t3.start();
         var t: [d] (string, string, string, string, string, string, real);
//         var t: [d] (string, string, string, string, int, int, real);
//         t3.stop();
//         t4.start();
         t.push_back((aTable: string, fromField: string, toField: string, wField: string, N.rows.get(i): string, N.cols.get(j): string, N.get(i,j): real));
  //       t.push_back((aTable: string, fromField: string, toField: string, wField: string, i: int, j: int, N.X(i,j): real));
//         t4.stop();
//         t5.start();
         pcon.execute(q, t);/*
         t5.stop();
         t1.stop();
         writeln("------------------");
         writeln("Defining d: ",t2.elapsed());
         writeln("Defining t: ",t3.elapsed());
         writeln("Push Back Time: ",t4.elapsed());
         writeln("Execute Time: ",t5.elapsed());
         writeln("Loop Time: ",t1.elapsed());
         writeln("------------------");*/
      }
     }
   }


  // BATCH PERSISTENCE
   proc persistSparseMatrix(con: Connection, batchsize: int, aTable: string
     , fromField: string, toField: string, weightField: string
     , X:[?D] real) {
     const q = "INSERT INTO %s (%s, %s, %s) VALUES (%s, %s, %s);";
     var cur = con.cursor();
     var count: int = 0;
     var dom: domain(1, int, false) = {1..0};
     var ts: [dom] (string, string, string, string, int, int, real);
     for ij in X.domain {
       var t: Timer;
       t.start();
       ts.push_back((aTable, fromField, toField, weightField, ij(1), ij(2), X(ij)));
       count += 1;
       if count >= batchsize {
         cur.execute(q, ts);
         count = 0;
         var reset: [dom] (string, string, string, string, int, int, real);
         ts = reset;
         t.stop();
         writeln("Batch Time: ",t.elapsed());
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
