import numpy as np
import json, random, re
from mrjob.job import MRJob
from mrjob.step import MRStep


def process_str2list(in_1):
    pass_1 = in_1.replace("\n", " ")
    pass_2 = re.split(r"[\.|\,|\(|\)|\s]+", pass_1)
    pass_3 = [word.lower() for word in pass_2]
    return pass_3[:-1]


def process_list2dict(in_1):
    pass_2 = []
    pass_1 = set(in_1)
    for i in pass_1:
        pass_2.append([i, in_1.count(i)])
    return {k:v for k,v in pass_2}


def randompick(json_in):
    rng = random.randrange(0, len(json_in))
    comp_sum = process_str2list(json_in[rng]['summary'])
    counted_sum = process_list2dict(comp_sum)
    return [json_in[rng]['id'], counted_sum]


def JacardCoef_1(t_a,t_b):
    sum_intersec = 0
    intersec = t_a.keys() & t_b.keys()
    for i in intersec:
        sum_intersec += min(t_a[i],t_b[i])
    sim = sum_intersec/(sum(t_a.values()) + sum(t_b.values()) - sum_intersec)
    return 1 - sim


def JacardCoef_2(t_a,t_b):
    dividend = np.intersect1d(t_a, t_b)
    divisor = np.union1d(t_a, t_b)
    sim = np.divide(len(dividend),len(divisor))
    return 1 - sim


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
        compared = randompick(x)
        for line in x:
            listified = process_str2list(line['summary'])
            counted_list = process_list2dict(listified)
            yield line["id"], (counted_list)

    def jaccard_sim(self, key, summary):
        if not key == compared[0]:
            for words in summary:
                result = JacardCoef_1(compared[1], words)
                yield None, (result, key)

    def reduce_max_sim(self, _, value):
        try:
            yield (compared[0], min(value)), "[Random Compared ID, [Max Jaccard Result, Max Match ID]]"
        except ValueError:
            pass


if __name__ == '__main__':
    MR_Jaccard.run()