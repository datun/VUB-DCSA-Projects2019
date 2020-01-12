import numpy as np
import json, random, re
from mrjob.job import MRJob
from mrjob.step import MRStep
from porter2stemmer import Porter2Stemmer
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from sklearn.metrics.pairwise import euclidean_distances
import random


def randompick(content):
    rng = random.randrange(0, len(content))
    return content[rng][0]



def text_stemmer(txt):
    stemmer = Porter2Stemmer()
    # Break the sentences into tokens
    # Tokens may not necessarily mean words; hence this cannot be done by re.split()
    token_words = word_tokenize(txt)
    stem_sentence = []
    for word in token_words:
        stem_sentence.append(stemmer.stem(word))
        stem_sentence.append(" ")
    return "".join(stem_sentence)



class MR_Cosine(MRJob):

    # For each article return the id, title and a reduced summary
    def mapper_raw(self, input_path, input_uri):
        global compared
        with open(input_path) as file:
            try:  # load JSON file given in that directory, if fails raise Exception
                x = json.load(file)
            except ValueError:
                raise Exception('Input file is not python loadable JSON file')

        for line in x:
            # Only keeping alphabet letters
            # Converting everything to lower case
            content = re.sub('[^a-z\s]', '' , line['summary'].lower())
            content = content.replace('\n', ' ').replace('\r', '')
            # stemming the content for better similarity calculation
            content = text_stemmer(content)

            title = line['title']
            title = title.replace('\n', '')
            yield None, (line['id'], title, content)

    # calculates the tf-idf vector of each article content
    def reducer(self, _ , id_title_content):
        tfidf = TfidfVectorizer(stop_words='english', norm=None)
        content_list = []
        title_ref_list = []
        counter = 0
        for id, title, content in id_title_content:
            content_list.append(content)
            title_ref_list.append([id, title, counter])
            counter += 1

        tfidf_vector = tfidf.fit_transform(content_list)
        cosine_sim = linear_kernel(tfidf_vector, tfidf_vector)

        x = randompick(title_ref_list)

        for id, title, counter in title_ref_list:
            if id == x:
                i = counter
                break

        article_sim = cosine_sim[i]
        sorted = np.argsort(article_sim)
        most_similar = sorted[1]
        id, title, c = title_ref_list[most_similar]
        yield id, title


if __name__ == '__main__':
    MR_Cosine.run()