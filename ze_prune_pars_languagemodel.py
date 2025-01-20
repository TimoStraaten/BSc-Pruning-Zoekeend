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


def pars_lm_pruning(name_in, name_out, lambda_param=0.3, max_iterations=10, tolerance=1e-6, prune_percentage=0.3):

    copy_file(name_in, name_out)

    con = duckdb.connect(name_out)

    print("Initializing algorithm")
    try:
        con.sql("""
        CREATE OR REPLACE TABLE store_probs AS
        SELECT 
            terms.docid,
            terms.termid,
            dict.term,
            COUNT(*) AS tf,
            docs.len AS doc_len,
            COUNT(*) * 1.0 / docs.len AS ptd,
            (dict.df * 1.0 / (SELECT sumdf FROM fts_main_documents.stats)) AS ptc
        FROM fts_main_documents.terms AS terms
        JOIN fts_main_documents.docs AS docs ON terms.docid = docs.docid
        JOIN fts_main_documents.dict AS dict ON terms.termid = dict.termid
        GROUP BY terms.docid, terms.termid, dict.term, dict.df, docs.len;
        """)

        for iteration in range(max_iterations):
            print(f"Starting iteration {iteration + 1}")

            con.sql(f"""
            CREATE OR REPLACE TABLE e_step AS
            SELECT 
                docid,
                termid,
                term,
                tf,
                doc_len,
                ptd,
                ptc,
                (tf * {lambda_param} * ptd) / 
                ((1 - {lambda_param}) * ptc + {lambda_param} * ptd) AS expected_weight
            FROM store_probs;

            CREATE OR REPLACE TABLE m_step AS
            SELECT
                docid,
                termid,
                term,
                expected_weight / SUM(expected_weight) OVER (PARTITION BY docid) AS updated_ptd
            FROM e_step;
            """)

            delta = con.execute("""
            SELECT MAX(ABS(updated_ptd - ptd)) AS max_delta
                FROM m_step
                JOIN store_probs USING (docid, termid);
            """).fetchone()[0]

            print(f"Iteration {iteration + 1}: delta = {delta}")
            if delta < tolerance:
                print("Convergence reached. Starting pruning.")
                con.sql("""
                CREATE OR REPLACE TABLE store_probs AS
                SELECT 
                    m.docid,
                    m.termid,
                    m.term,
                    e.tf,
                    e.doc_len,
                    m.updated_ptd AS ptd,
                    e.ptc
                FROM m_step AS m
                JOIN e_step AS e USING (docid, termid);
                """)
                break

            con.sql("""
            CREATE OR REPLACE TABLE store_probs AS
            SELECT 
                m.docid,
                m.termid,
                m.term,
                e.tf,
                e.doc_len,
                m.updated_ptd AS ptd,
                e.ptc
            FROM m_step AS m
            JOIN e_step AS e USING (docid, termid);
            """)

    finally:
        con.sql("""
        DROP TABLE IF EXISTS e_step;
        DROP TABLE IF EXISTS m_step;
        DROP TABLE IF EXISTS terms_to_delete;
        """)

    con.sql(f"""
    CREATE TEMP TABLE terms_to_delete AS
    SELECT CONCAT(docid, '_', termid) AS unique_key
    FROM store_probs
    ORDER BY ptd ASC
    LIMIT CAST((SELECT COUNT(*) * {prune_percentage} FROM store_probs) AS INT);

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

    DROP TABLE IF EXISTS store_probs;
    DROP TABLE IF EXISTS terms_to_delete;     
    """)
    con.close()

    print("Parsimonious Language Model Pruning Completed. Database saved to {name_out}")


if __name__ == "__main__":
    pars_lm_pruning('full_index.db', 'parslm_pruned.db')
