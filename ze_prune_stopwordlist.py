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


def stopwordlist_pruning(name_in, name_out, stopwords=None):

    copy_file(name_in, name_out)

    con = duckdb.connect(name_out)

    # Default stopwords if none are provided
    if stopwords is None:
        stopwords = [
            'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and',
            'any', 'are', 'aren’t', 'as', 'at', 'be', 'because', 'been', 'before', 'being',
            'below', 'between', 'both', 'but', 'by', 'can', 'can’t', 'cannot', 'could', 'couldn’t',
            'did', 'didn’t', 'do', 'does', 'doesn’t', 'doing', 'don’t', 'down', 'during', 'each',
            'few', 'for', 'from', 'further', 'had', 'hadn’t', 'has', 'hasn’t', 'have', 'haven’t',
            'having', 'he', 'he’d', 'he’ll', 'he’s', 'her', 'here', 'here’s', 'hers', 'herself',
            'him', 'himself', 'his', 'how', 'how’s', 'i', 'i’d', 'i’ll', 'i’m', 'i’ve', 'if',
            'in', 'into', 'is', 'isn’t', 'it', 'it’s', 'its', 'itself', 'let’s', 'me', 'more',
            'most', 'mustn’t', 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once',
            'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own',
            'same', 'shan’t', 'she', 'she’d', 'she’ll', 'she’s', 'should', 'shouldn’t', 'so', 'some',
            'such', 'than', 'that', 'that’s', 'the', 'their', 'theirs', 'them', 'themselves', 'then',
            'there', 'there’s', 'these', 'they', 'they’d', 'they’ll', 'they’re', 'they’ve', 'this',
            'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', 'wasn’t',
            'we', 'we’d', 'we’ll', 'we’re', 'we’ve', 'were', 'weren’t', 'what', 'what’s', 'when',
            'when’s', 'where', 'where’s', 'which', 'while', 'who', 'who’s', 'whom', 'why', 'why’s',
            'with', 'won’t', 'would', 'wouldn’t', 'you', 'you’d', 'you’ll', 'you’re', 'you’ve',
            'your', 'yours', 'yourself', 'yourselves', 'x', 'y', 'your', 'yours', 'yourself',
            'yourselves', 'you', 'yond', 'yonder', 'yon', 'ye', 'yet', 'z', 'zillion', 'j',
            'u', 'umpteen', 'usually', 'us', 'username', 'uponed', 'upons', 'uponing', 'upon',
            'ups', 'upping', 'upped', 'up', 'unto', 'until', 'unless', 'unlike', 'unliker',
            'unlikest', 'under', 'underneath', 'use', 'used', 'usedest', 'r', 'rath', 'rather',
            'rathest', 'rathe', 're', 'relate', 'related', 'relatively', 'regarding', 'really',
            'res', 'respecting', 'respectively', 'q', 'quite', 'que', 'qua', 'n', 'neither',
            'neaths', 'neath', 'nethe', 'nethermost', 'necessary', 'necessariest', 'necessarier',
            'never', 'nevertheless', 'nigh', 'nighest', 'nigher', 'nine', 'noone', 'nobody',
            'nobodies', 'nowhere', 'nowheres', 'no', 'noes', 'nor', 'nos', 'no-one', 'none',
            'not', 'notwithstanding', 'nothings', 'nothing', 'nathless', 'natheless', 't', 'ten',
            'tills', 'till', 'tilled', 'tilling', 'to', 'towards', 'toward', 'towardest', 'towarder',
            'together', 'too', 'thy', 'thyself', 'thus', 'than', 'that', 'those', 'thou', 'though',
            'thous', 'thouses', 'thoroughest', 'thorougher', 'thorough', 'thoroughly', 'thru',
            'thruer', 'thruest', 'thro', 'through', 'throughout', 'throughest', 'througher',
            'thine', 'this', 'thises', 'they', 'thee', 'the', 'then', 'thence', 'thenest',
            'thener', 'them', 'themselves', 'these', 'therer', 'there', 'thereby', 'therest',
            'thereafter', 'therein', 'thereupon', 'therefore', 'their', 'theirs', 'thing', 'things',
            'three', 'two', 'o', 'oh', 'owt', 'owning', 'owned', 'own', 'owns', 'others',
            'other', 'otherwise', 'otherwisest', 'otherwiser', 'of', 'often', 'oftener',
            'oftenest', 'off', 'offs', 'offest', 'one', 'ought', 'oughts', 'our', 'ours',
            'ourselves', 'ourself', 'out', 'outest', 'outed', 'outwith', 'outs', 'outside',
            'over', 'overallest', 'overaller', 'overalls', 'overall', 'overs', 'or', 'orer',
            'orest', 'on', 'oneself', 'onest', 'ons', 'onto', 'a', 'atween', 'at', 'athwart',
            'atop', 'afore', 'afterward', 'afterwards', 'after', 'afterest', 'afterer', 'ain',
            'an', 'any', 'anything', 'anybody', 'anyone', 'anyhow', 'anywhere', 'anent',
            'anear', 'and', 'andor', 'another', 'around', 'ares', 'are', 'aest', 'aer',
            'against', 'again', 'accordingly', 'abaft', 'abafter', 'abaftest', 'abovest',
            'above', 'abover', 'abouter', 'aboutest', 'about', 'aid', 'amidst', 'amid',
            'among', 'amongst', 'apartest', 'aparter', 'apart', 'appeared', 'appears',
            'appear', 'appearing', 'appropriating', 'appropriate', 'appropriatest', 'appropriates',
            'appropriater', 'appropriated', 'already', 'always', 'also', 'along', 'alongside',
            'although', 'almost', 'all', 'allest', 'aller', 'allyou', 'alls', 'albeit',
            'awfully', 'as', 'aside', 'asides', 'aslant', 'ases', 'astrider', 'astride',
            'astridest', 'astraddlest', 'astraddler', 'astraddle', 'availablest', 'availabler',
            'available', 'aughts', 'aught', 'vs', 'v', 'variousest', 'variouser', 'various',
            'via', 'vis-a-vis', 'vis-a-viser', 'vis-a-visest', 'viz', 'very', 'veriest',
            'verier', 'versus', 'k', 'g', 'go', 'gone', 'good', 'got', 'gotta', 'gotten',
            'get', 'gets', 'getting', 'b', 'by', 'byandby', 'by-and-by', 'bist', 'both',
            'but', 'buts', 'be'
        ]
    # Format the stopwords
    stopwords_sql = ", ".join([f"('{word}')" for word in stopwords])

    con.sql(f"""
        WITH stopwords(stopword) AS (
            VALUES 
            {stopwords_sql}
        )
        
        DELETE FROM fts_main_documents.dict
        WHERE term IN (SELECT stopword FROM stopwords);

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
    """)

    con.close()
    print(f"Stopwords based Pruning Completed. Database saved to {name_out}")


if __name__ == "__main__":
    stopwordlist_pruning('full_index.db', 'stopword_pruned.db')