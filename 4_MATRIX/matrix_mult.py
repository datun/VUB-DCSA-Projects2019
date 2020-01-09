from mrjob.job import MRJob
from mrjob.step import MRStep
import os


row_counter = -1
column_counter = -1


class MR_MatrixMult(MRJob):

    def steps(self):
        return [MRStep(mapper=self.map_mats,
                       reducer=self.red_multip),
                MRStep(mapper=self.map_result,
                       reducer=self.red_sumresult)]

    def map_mats(self, _, line):
        global row_counter
        global column_counter
        mat_in = line.split()
        filename = os.environ['map_input_file']

        # For matrix A (i,k)
        if 'A.txt' in filename:
            row_counter += 1
            for k in range(len(mat_in)):
                yield row_counter, ("A", k, mat_in[k])
        # For matrix B (k,j)
        if 'B.txt' in filename:
            column_counter += 1
            for j in range(len(mat_in)):
                yield column_counter, ("B", j, mat_in[j])

    def red_multip(self, key, val):
        listA = []
        listB = []
        for temp in val:
            if temp[0] == "A":
                listA.append(temp)
            else:
                listB.append(temp)

        for a in listA:
            for b in listB:
                yield (a[1],b[1]), (float(a[2]) * float(b[2]))

    def map_result(self, key, val):
        yield (key, val)

    def red_sumresult(self, key, val):
        yield (key, sum(val))


if __name__ == '__main__':
    MR_MatrixMult.run()
