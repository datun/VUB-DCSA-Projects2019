from mrjob.job import MRJob
import re
import nltk

# Status:
# Picking rows with correct values [X]
# Processing titles [X]
# Processing titles correctly [X]
# Removing NLTK stuff [X]
# ███████ Assumed to be DONE ████████

# ███ What is janky in this code?
# 1- Using non-whitespace and non-digits more than 2 characters to reduce expressions.
# This was done to fight against " L' ", " d' "," 's " like prefixes and postfixes
# 2- Output of the MRJob
# Due to the multiple languages within titles and how MRJob handles those files, the output
# files contain decoded unicode strings such as "Açúcar" => "A\u00e7\u00facar"


class MRWordFreqCount(MRJob):
    def mapper(self, _, line):
        pre_process = re.split(r"[\t]", line)  # Splitting files according to tabs
        if pre_process[1] in ("movie", "short"):  # 2nd column shows the type
            post_process = re.findall("[^\W\d]{2,}", pre_process[2])
            # Find non-whitespace and non-digits with more than 2 characters (to prevent 's 'l stuff)
            lang_process = " ".join(post_process)  # joining the results for token process
            tokens = nltk.word_tokenize(lang_process)  # 3rd column contains the original movie name
            tagged = nltk.pos_tag(tokens)  # words are tokenized and tagged
            for tagged, tags in tagged:
                # Only accepting words that are not
                # IN=Prepositions, DT=Determiner, CONJ=Conjunctions, SYM=Symbols, AT=Articles
                if tags not in ("IN", "AT", "CONJ", "SYM", "DT", "POS", "RB", "PRT", "NUM", "."):
                    yield (tagged, 1)

    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        yield (word, sum(counts))


if __name__ == '__main__':
    MRWordFreqCount.run()
