import sqlite3
import time
from collections import defaultdict
from collections import Counter
from operator import itemgetter
run_part = [0,1,0]


# String seperation (supplied)
def find_words(s):
    symbols = ['\n', '`', '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '-', '+', '=', '{', '[', ']', '}',
               '|', '\\', ':', ';', '"', "'", '<', '>', '.', '?', '/', ',']
    s = s.lower()
    for sym in symbols:
        s = s.replace(sym, " ")
    words = set()
    for w in s.split(" "):
        if len(w.replace(" ", "")) > 0:
            words.add(w)
    return words


conn = sqlite3.connect('reddit.db')
conn.text_factory = str  # To prevent utf-8 encoding error

cur = conn.cursor()

## First part
if run_part[0]:  # 11015 sec (3 hours)
    start_time = time.time()
    vocab = defaultdict(set)

    for subred in cur.execute("SELECT subreddits.name, com.body FROM comments AS com INNER JOIN subreddits ON com.subreddit_id = subreddits.id"):
        vocab[subred[0]] |= find_words(subred[1])

    # sort dictionary
    sorted = dict(Counter(vocab).most_common(10))
    print("--- %s seconds ---" % (time.time() - start_time))
    for item in vocab:
        print item, len(vocab[item])

vocab = None
sorted = None

## Second part
if run_part[1]:
    start_time2 = time.time()
    results = defaultdict(set)
    ## Second part
    count = 0
    current_subreddit = []
    current_set = set({})
    for subred in cur.execute("SELECT DISTINCT authors.name, subreddits.name FROM comments AS com INNER JOIN authors ON com.author_id = authors.id INNER JOIN subreddits ON com.subreddit_id = subreddits.id ORDER BY subreddits.name"):
        # timing the SQL merging
        if count == 0:
            current_subreddit = subred[0]
            count += 1
            time2 = time.time()
            print("--- SQL %s seconds ---" % (time2 - start_time2))

        if current_subreddit != subred[0]:
            results[current_subreddit] = current_set
            current_set = set(subred[1])
        else:
            current_set |= set(subred[1])

        current_subreddit = subred[0]
    results[current_subreddit] = current_set

    comparelist = {}
    for key in results:
        for key2 in results:
            if key != key2:
                comparelist[key+' and '+key2] = len(results[key] & results[key2])

    final = dict(sorted(comparelist.iteritems(), key=itemgetter(1), reverse=True)[:20])

    print("--- %s seconds ---" % (time.time() - start_time2))
    print

result = None
sorted1 = None
final = None
comparelist = None
## Deepest subreddit
if run_part[2]:  # 11015 sec (3 hours)
    start_time = time.time()
    vocab = defaultdict(set)
    for subred in cur.execute("SELECT id FROM comments INNER JOIN parent_id ON id"):
        print subred


    print("--- %s seconds ---" % (time.time() - start_time))








