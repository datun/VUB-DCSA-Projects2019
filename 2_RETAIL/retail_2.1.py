from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter

class MRBestCustomer(MRJob):
    def mapper(self, _, line):
        try:
            pre_process = line.split(',')
            # The description already have commas.
            # So the fields need to be extracted from the right
            length = len(pre_process)
            id = pre_process[length - 2]
            revenue = float(pre_process[length - 3]) * float(pre_process[ length - 5])
            date = pre_process[length - 4].split(" ")
            # Last two digits of the year field.
            year = 2000 + int(date[0][-2] + date[0][-1])
            yield ((id , year) , revenue)

        except:
            yield (None ,0)

    def combiner(self , comb_id , revenue):
        if comb_id != None:
            id, year = comb_id
            total_rev = sum(revenue)
            yield(year , dict({id:total_rev}))

    def reducer(self , year, rev_dict):
        combined_dict = {}
        for rev in rev_dict:
            combined_dict = Counter(combined_dict) + Counter(rev)

        # combined_dict = dict(combined_dict)
        f1 = combined_dict.most_common(10)

        yield year , f1





if __name__ == '__main__':
    MRBestCustomer.run()