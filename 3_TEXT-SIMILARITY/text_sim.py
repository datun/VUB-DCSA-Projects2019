import numpy as np
import json, random, re
from mrjob.job import MRJob
from mrjob.step import MRStep


def process_str2list(in_1):
    pass_1 = in_1.replace("\n", " ")
    pass_2 = re.split(r"[\.|\,|\(|\)|\s]+", pass_1)
    return pass_2[:-1]


def randompick(json_in):
    rng = random.randrange(0, len(json_in))
    comp_sum = process_str2list(json_in[rng]['summary'])
    return [json_in[rng]['id'], comp_sum]


def JacardCoef_2(t_a,t_b):
    # According to wikipedia intersection description, calculation by list-similarity.
    dividend = np.intersect1d(t_a, t_b)
    divisor = np.union1d(t_a, t_b)
    sim = np.divide(len(dividend),len(divisor))
    return sim


compared = []


class MR_Jaccard(MRJob):
    def steps(self):

        #     Two ways of doing the same job!
        #     Step 1: Load the file and parse summaries
        #     Step 2: Pick a random summary to be compared one
        #     Step 3: Run the chosen file through MR to find the word frequencies, hold it in memory.
        #     Step 4: Start word frequencies of rest of the list and using given function find similarities and store it
        #     Step 5: Output result
        #
        #     Better way or say the simpler one
        #     Step 1: Load file and parse summaries only.
        #     Step 2: Calculate word frequencies for all files.
        #     Step 3: Save or output those results to be used in next part (transmission)
        #     Step 4: Pick a random summary
        #     Step 5: Run the function of random summary with rest of the summary
        #     Step 6: Output result
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
            yield line["id"], (listified)

    def jaccard_sim(self, key, summary):
        if not key == compared[0]:
            for words in summary:
                result = JacardCoef_2(compared[1], words)
                yield None, (result, key)

    def reduce_max_sim(self, _, value):
        try:
            yield (compared[0], max(value)), "[Random Compared ID, [Max Jaccard Result, Max Match ID]]"
        except ValueError:
            pass


if __name__ == '__main__':
    MR_Jaccard.run()