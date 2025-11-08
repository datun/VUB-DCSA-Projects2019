from mrjob.job import MRJob
from mrjob.step import MRStep

# Global values to store number of rows and columns of input matrices
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

        # mapping elements of input matrices with tags that will be used in reducer
        # also storing the dimensions of columns and rows of inputs
        # the reason for storing like this is explained in report, we are only mapping matrix elements with indicators
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
        # due to the output of mapper, we do not need duplicates of a matrix element.
        # instead here we calculate every combination of multiplication of elements that are required
        # for example: A[i,k] and B[k,j] are yielded as (i,j), A[i,k] * B[k,j]
        listA = []
        listB = []
        for temp in val:
            if temp[0] == "A":
                listA.append(temp)
            elif temp[0] == "B":
                listB.append(temp)

        for a in listA:
            for b in listB:
                # yielding a key that represents which values are to be summed for multiplication to be finalised
                yield (a[1], b[1]), float(a[2]) * float(b[2])

    def map_result(self, key, val):
        # mapping the previous result for reducer to work with
        yield key, val

    def red_sumresult(self, key, val):
        # taking identical keys and summing them together, creating an [i,j] value for output matrix.
        yield (key, sum(val))


if __name__ == '__main__':
    MR_MatrixMult.run()
