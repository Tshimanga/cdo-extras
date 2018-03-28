DROP TABLE IF EXISTS r.cho_names;
CREATE TABLE r.cho_names
(ftr_id int, name text);
INSERT INTO r.cho_names (ftr_id, name) VALUES
(1, 'star lord'),
(2, 'gamora'),
(3, 'groot'),
(4, 'drax'),
(5, 'rocket'),
(6, 'mantis'),
(7, 'yondu'),
(8, 'nebula');

DROP TABLE IF EXISTS r.cho_edges;
CREATE TABLE r.cho_edges
(from_fid int, to_fid int, w numeric);
INSERT INTO r.cho_edges VALUES
(1, 2, 1.0),
(1, 3, 1.0),
(1, 4, 1.0),
(2, 2, 1.0),
(2, 4, 1.0),
(3, 4, 1.0),
(4, 5, 1.0),
(5, 6, 1.0),
(6, 7, 1.0),
(6, 8, 1.0),
(7, 8, 1.0);
