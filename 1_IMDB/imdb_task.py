from mrjob.job import MRJob
import re

# WORD_RE = re.compile(r"[^\s]+")


# re.compile creates a pattern to search/use. ( \w = a-zA-Z0-9_ )
# re.split, splits the input string when it matches input pattern ( \t = split at every tab)
# Check if we can process line by line, split into columns and match column[1] with "movie" or "short"
# Then split column[2] wrt whitespaces to obtain word list

# Status:
# Picking rows with correct values [X]
# Processing titles [X]
# Processing titles correctly [ ] (see 1.1)
# Removing NLTK stuff [ ]

# 1.1: Current implementation uses regex to pattern-match desired characters from title
# First problem is Turkish and French characters, Second is unicode processing.
# TR and FR characters are not processed as they exist on file, thus resulting in gibberish numbers
# sabotaging the mapper process. There are shorts/movies with #,' or other characters.
# removing # is easy, but ' proves difficult due to language. Ex: french articles L'
# This requires NLTK.

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        pre_process = re.split(r"[\t]", line)
        if pre_process[1] in ("movie", "short"):
            for word in pre_process[2].split():
            # for word in WORD_RE.findall(pre_process[2]):
                yield (word, 1)

    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        yield (word, sum(counts))


if __name__ == '__main__':
    MRWordFreqCount.run()
