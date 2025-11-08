from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import nltk
from nltk.corpus import stopwords
# setting stopwords from multiple languages to fix the problems caused by titles from different languages
stopwords = set(stopwords.words(["arabic", "azerbaijani", "danish", "dutch", "english", "finnish", "french", "german",
                                 "greek", "hungarian", "indonesian", "italian", "kazakh", "nepali", "norwegian",
                                 "portuguese", "romanian", "russian","slovene", "spanish", "swedish",
                                 "tajik", "turkish"]))
# global counter to sort out the output
counter = -1
# global list for sorting purposes
val_list = []

class MRWordFreqCount(MRJob):
    def steps(self):
        return [MRStep(mapper=self.mapper,          # counts the words from titles
                       combiner=self.combiner,      # sums the words from mapper
                       reducer=self.reducer),       # changes the output for sorting (also sums if anything is left)
                MRStep(reducer=self.sorter)]        # sorts the values into top 50

    def mapper(self, _, line):
        pre_process = re.split(r"[\t]", line)  # Splitting files according to tabs
        if pre_process[1] in ("movie", "short"):  # 2nd column shows the type
            post_process = re.findall("[^\W\d]{2,}", pre_process[2].lower())
            # Find non-whitespace and non-digits with more than 2 characters (to prevent 's 'l stuff)
            lang_process = " ".join(post_process)  # joining the results for token process
            tokens = nltk.word_tokenize(lang_process)  # 3rd column contains the original movie name
            tagged = nltk.pos_tag(tokens)  # words are tokenized and tagged
            for tagged, tags in tagged:
                # Only accepting words that are not
                # IN=Prepositions, DT=Determiner, CONJ=Conjunctions, SYM=Symbols, AT=Articles and so on
                if tags not in ('IN', 'AT', 'CONJ', 'SYM', 'DT', 'POS', 'RB', 'PRT', 'NUM', 'TO', 'CC', '.'):
                    if tagged not in stopwords:
                        yield (tagged, 1)

    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        yield None, (sum(counts), word)

    def sorter(self, _, result):
        global val_list
        for counts, word in result:
            # adding first 50 values to fill a list
            if len(val_list) <= 50:
                val_list.append((int(counts), word))
            # when 50 is reached, removing minimum values if there exists a higher value
            # and appending that higher value
            else:
                if min(val_list)[0] < int(counts):
                    val_list.remove(min(val_list))
                    val_list.append((int(counts),word))
        for i in sorted(val_list, reverse=True):
            yield i


if __name__ == '__main__':
    MRWordFreqCount.run()
