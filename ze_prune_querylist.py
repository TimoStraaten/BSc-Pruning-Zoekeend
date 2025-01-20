import duckdb
import pathlib
import os


def copy_file(name_in, name_out):
    path1 = pathlib.Path(name_in)
    if not path1.is_file():
        raise ValueError(f"File {name_in} does not exist.")
    path2 = pathlib.Path(name_out)
    if path2.is_file():
        raise ValueError(f"File {name_out} already exists.")
    path2.write_bytes(path1.read_bytes())


def querylist_pruning(name_in, name_out, query_file_path):
    copy_file(name_in, name_out)

    con = duckdb.connect(name_out)

    con.sql(f"""
    CREATE TEMPORARY TABLE unique_query_terms AS
    SELECT DISTINCT lower(trim(tmp_term)) AS term
    FROM (
        SELECT unnest(string_split(lower(trim(content)), ' ')) AS tmp_term
        FROM read_text('{os.getcwd()}/{query_file_path}')
    )
    WHERE length(term) > 1;
        
    CREATE TEMPORARY TABLE terms_to_delete AS 
    SELECT d.termid, d.term
    FROM fts_main_documents.dict d
    LEFT JOIN unique_query_terms uqt ON lower(d.term) = uqt.term
    WHERE uqt.term IS NULL;
        
    DELETE FROM fts_main_documents.dict
    WHERE termid IN (SELECT termid FROM terms_to_delete);
        
    DELETE FROM fts_main_documents.terms
    WHERE termid NOT IN (SELECT termid FROM fts_main_documents.dict);
        
    UPDATE fts_main_documents.docs
    SET len = (
        SELECT COUNT(*)
        FROM fts_main_documents.terms t
        WHERE t.docid = docs.docid
    );
        
    UPDATE fts_main_documents.stats
    SET 
        avgdl = (SELECT AVG(len) FROM fts_main_documents.docs),
        sumdf = (SELECT SUM(df) FROM fts_main_documents.dict);
        
    DROP TABLE unique_query_terms;
    DROP TABLE terms_to_delete;
    """)

    con.close()
    print(f"QueryList based Pruning Completed. Database saved to {name_out}")


if __name__ == "__main__":
    querylist_pruning('full_index.db', 'querylist_pruned.db', 'test_query.txt')
