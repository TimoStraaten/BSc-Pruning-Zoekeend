import duckdb
import pathlib


def copy_file(name_in, name_out):
    path1 = pathlib.Path(name_in)
    if not path1.is_file():
        raise ValueError(f"File {name_in} does not exist.")
    path2 = pathlib.Path(name_out)
    if path2.is_file():
        raise ValueError(f"File {name_out} already exists.")
    path2.write_bytes(path1.read_bytes())


def small_lm_pruning(name_in, name_out, lambda_param=0.3):

    copy_file(name_in, name_out)

    con = duckdb.connect(name_out)

    con.sql(f"""
    WITH term_freqs AS (
        SELECT 
            docid, 
            termid,
            COUNT(*) AS tf
        FROM fts_main_documents.terms
        GROUP BY docid, termid
        HAVING COUNT(*) > 0
    ),
    scores AS (
        SELECT 
            tf.docid,
            tf.termid,
            tf.tf * LOG((({lambda_param}) * (tf.tf / doc.len)) / ((1 - {lambda_param}) * (dict.df / s.sumdf))) AS score
        FROM term_freqs tf
        JOIN fts_main_documents.docs doc ON tf.docid = doc.docid
        JOIN fts_main_documents.dict dict ON tf.termid = dict.termid
        CROSS JOIN fts_main_documents.stats s
    ),
    terms_to_delete AS (
        SELECT 
            CONCAT(docid, '_', termid) AS unique_key
        FROM scores
        WHERE score <= 0
    )

    DELETE FROM fts_main_documents.terms
    WHERE CONCAT(docid, '_', termid) IN (
        SELECT unique_key 
        FROM terms_to_delete
    );

    UPDATE fts_main_documents.docs
    SET len = (
        SELECT COUNT(*)
        FROM fts_main_documents.terms t
        WHERE t.docid = docs.docid
    );

    UPDATE fts_main_documents.dict
    SET df = (
        SELECT COUNT(DISTINCT docid)
        FROM fts_main_documents.terms
        WHERE termid = dict.termid
    );

    UPDATE fts_main_documents.stats
    SET 
        avgdl = (SELECT AVG(len) FROM fts_main_documents.docs),
        sumdf = (SELECT SUM(df) FROM fts_main_documents.dict);
    """)

    con.close()
    print(f"Small Language Model Pruning Completed. Database saved to {name_out}")


if __name__ == "__main__":
    small_lm_pruning('full_index.db', 'smalllm_pruned.db')
