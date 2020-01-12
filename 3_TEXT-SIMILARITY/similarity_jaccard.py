import numpy as np  # is not used, here to show a first basic implementation
import json, random, re  # json is used to load the file, random is used to pick a random paper
from mrjob.job import MRJob
from mrjob.step import MRStep


# conversion from string summary into a list
def process_str2list(in_1):
    pass_1 = in_1.replace("\n", " ")    # replace all newlines with spaces
    pass_2 = re.split(r"[\.|\,|\(|\)|\s]+", pass_1)  # use regex to remove . , ( ) and any whitespace character
    pass_3 = [word.lower() for word in pass_2]  # lower output for comparison reasons
    return pass_3[:-1]


# instead of running another word counter with MR as in task 1.1, here is a small implementation
def process_list2dict(in_1):
    pass_2 = []
    pass_1 = set(in_1)  # using set to remove identical values
    for i in pass_1:
        pass_2.append([i, in_1.count(i)])   # using the original one with unique ones to count occurrences
    return {k:v for k,v in pass_2}   # converting the list to a dictionary for ease of access


# picking a random paper as requested in the pdf
def randompick(json_in):
    rng = random.randrange(0, len(json_in))     # picking a random number from range of papers
    # below is a process that all papers go through. converting summary string to a dictionary
    comp_sum = process_str2list(json_in[rng]['summary'])
    counted_sum = process_list2dict(comp_sum)
    return [json_in[rng]['id'], counted_sum]


# Implementation of Jacard Distance (subtracting from 1 yields distance) where weights are considered.
def JacardCoef_1(t_a,t_b):
    sum_intersec = 0
    intersec = t_a.keys() & t_b.keys()   # using dictionary keys to yield intersection
    for i in intersec:
        # taking minimum of intersection values since intersecting words may occur different times in both summaries
        sum_intersec += min(t_a[i],t_b[i])
    # calculation according to the paper of Anna Huang - Similarity Measures for Text Document Clustering
    sim = sum_intersec/(sum(t_a.values()) + sum(t_b.values()) - sum_intersec)
    return 1 - sim


# Bad implementation which doesn't take consider words occurring more than once, left as an example
def JacardCoef_2(t_a,t_b):
    dividend = np.intersect1d(t_a, t_b)
    divisor = np.union1d(t_a, t_b)
    sim = np.divide(len(dividend),len(divisor))
    return 1 - sim


# Global value to store randomly picked paper
compared = []


class MR_Jaccard(MRJob):
    def steps(self):
        return [
                MRStep(mapper_raw=self.mapper_raw,       # Load JSON file and map id with summaries
                       combiner=self.jaccard_sim,        # Calculate similarities
                       reducer=self.reduce_max_sim)      # Output Max
               ]

    def mapper_raw(self, input_path, input_uri):
        global compared
        with open(input_path) as file:
            try:  # load JSON file given in that directory, if fails raise Exception
                x = json.load(file)
            except ValueError:
                raise Exception('Input file is not python loadable JSON file')
        compared = randompick(x)    # Picking of random paper to be compared
        for line in x:
            # converting every summary into dictionary with weights
            # yielding paper id and dictionary
            listified = process_str2list(line['summary'])
            counted_list = process_list2dict(listified)
            yield line["id"], (counted_list)

    def jaccard_sim(self, key, summary):
        if not key == compared[0]:  # skipping picked summary
            for words in summary:
                result = JacardCoef_1(compared[1], words)   # comparing the input with picked summary
                yield None, (result, key)   # yielding none, (result,key) to process min value

    def reduce_max_sim(self, _, value):
        try:
            # taking min(value) because 1 in Jaccard Distance means compared results are disjoint. (Jaccard Coef = 0)
            # When two identical strings are compared, Jaccard distance would yield 0. (Jaccard Coef = 1)
            yield (compared[0], min(value)), "[Random Compared ID, [Max Jaccard Result, Max Match ID]]"
        except ValueError:
            pass


if __name__ == '__main__':
    MR_Jaccard.run()