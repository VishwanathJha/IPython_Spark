from pyspark import SparkContext, SparkConf
from operator import add
import re
import string

APP_NAME = "PythonWordCount"

def removePunctuation(text):
    """Removes punctuation, changes to lowercase, and strips leading and trailing spaces.
    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated. (e.g. it's becomes its)
    Args:
        text (str): A string.
    Returns:
        str: The cleaned up string.
    """
    return re.sub('[^a-z| |0-9]','', text.lower())

def main(sc):
    raw_text = sc.textFile("/Users/vishwanath.jha/Downloads/shakespeare.txt",4)
    tokens = raw_text.flatMap(lambda x: x.split(" "))
    #tokens.take(5)
    counts = tokens.map(lambda x: removePunctuation(x)).map(lambda x: (x, 1)).reduceByKey(add).collect()
    for (word, count) in counts:
        print "%s: %i" % (word, count)

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[*]")
    conf = conf.setAppName(APP_NAME)
    sc   = SparkContext(conf=conf)

    # Execute Main functionality
    main(sc)
    