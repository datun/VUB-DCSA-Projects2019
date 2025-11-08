from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter
import re
import nltk



class MRGenreWordCount(MRJob):
    # Creates Genre , title pairs
    def mapper1(self, _, line):
        # line = line.decode('utf-8')
        pre_process = re.split(r"[\t]", line)  # Splitting files according to tabs
        if pre_process[1] in ("movie"):  # 2nd column shows the type
            # Multiple genres associated with each movie
            # Genres are comma separated
            genre = re.split(r"[,]", pre_process[8])  # Splitting files according to tabs
            title = pre_process[2]
            for key in genre:
                # Strip quotations and \N character"
                key = key.strip('\"\\N')
                # Only return movies that have genres associated with them
                if key != "":
                    yield (key, title)

    # Creates Genre , {word counts} pairs
    def mapper2(self, key, title):
        post_process = re.findall("[^\W\d]{2,}", title)
        # Find non-whitespace and non-digits with more than 2 characters (to prevent 's 'l stuff)
        lang_process = " ".join(post_process)  # joining the results for token process
        tokens = nltk.word_tokenize(lang_process)  # 3rd column contains the original movie name
        tagged = nltk.pos_tag(tokens)  # words are tokenized and tagged
        count = []
        for word, tags in tagged:
            # Only accepting words that are not
            # IN=Prepositions, DT=Determiner, CONJ=Conjunctions, SYM=Symbols, AT=Articles
            # yield (word, 1)
            if tags not in ("IN", "AT", "CONJ", "SYM", "DT", "POS", "RB", "PRT", "NUM", ".", "CC"):
                count.append(word)

        count_dict = Counter(count)
        yield (key, count_dict)

    def combiner(self, key, count_dict):
        final_dict = {}
        for cd in count_dict:
            final_dict = Counter(final_dict) + Counter(cd)

        yield (key, dict(final_dict))

    # Creates an union of all dictionaries associated with a particular genre.
    # Finds top 15 key-words for each genre from the associated dictionaries.
    def reducer1(self, key, count_dict):
        final_dict = {}
        for cd in count_dict:
            final_dict = Counter(final_dict) + Counter(cd)
        f1 = final_dict.most_common(15)

        yield (key, f1)


    def steps(self):
        return [
            MRStep(mapper = self.mapper1) ,
            MRStep(mapper = self.mapper2, combiner = self.combiner, reducer = self.reducer1)
        ]

if __name__ == '__main__':
    MRGenreWordCount.run()
