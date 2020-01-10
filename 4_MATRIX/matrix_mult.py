from mrjob.job import MRJob
from mrjob.step import MRStep

dim_Arows = -1
dim_Acolumns = 0
dim_Brows = -1
dim_Bcolumns = 0


class MR_MatrixMult(MRJob):
    def steps(self):
        return [MRStep(mapper_raw=self.map_values,      # Mapping the matrix elements to be able to process better
                       reducer=self.red_multip),        # Multiplying and tagging elements to sum in the next step
                MRStep(mapper=self.map_result,          # Mapping the tags -> key
                       reducer=self.red_sumresult)]     # Summing the elements with same key, then it is done!

    def map_values(self,input_path, input_uri):
        global dim_Arows
        global dim_Acolumns
        global dim_Brows
        global dim_Bcolumns
        x = open(input_path,"r")

        for line in x:
            row_in = line.split()
            if 'A.txt' in input_path:
                if dim_Acolumns == 0:
                    dim_Acolumns = len(row_in) -1
                dim_Arows += 1
                for j in range(len(row_in)):
                    yield j, ("A", dim_Arows,row_in[j])
            if 'B.txt' in input_path:
                if dim_Bcolumns == 0:
                    dim_Bcolumns = len(row_in) -1
                dim_Brows += 1
                for j in range(len(row_in)):
                    yield dim_Brows, ("B", j, row_in[j])

    def red_multip(self, key, val):
        listA = []
        listB = []
        for temp in val:
            if temp[0] == "A":
                listA.append(temp)
            elif temp[0] == "B":
                listB.append(temp)

        for a in listA:
            for b in listB:
                yield (a[1], b[1]), float(a[2]) * float(b[2])

    def map_result(self, key, val):
        yield key, val

    def red_sumresult(self, key, val):
        yield (key, sum(val))


if __name__ == '__main__':
    MR_MatrixMult.run()
