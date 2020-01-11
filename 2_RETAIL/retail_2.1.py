# import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep
from collections import Counter

class MRBestCustomer(MRJob):
    # pre-processing
    def mapper(self, _, line):
        try:
            pre_process = line.split(',')
            # The description already have commas.
            # So the fields need to be extracted from the right
            length = len(pre_process)
            invoice = pre_process[0]
            stock_code = pre_process[1]
            id = pre_process[length - 2]
            revenue = float(pre_process[length - 3]) * float(pre_process[ length - 5])
            date = pre_process[length - 4].split(" ")
            # Last two digits of the year field.
            year = 2000 + int(date[0][-2] + date[0][-1])
            yield ((invoice, stock_code, id , year) , revenue)

        except:
            yield (None ,0)

    def combiner(self,  comb_id , revenue):
        if comb_id != None:
            invoice, stock_code, id, year = comb_id
            # Discarding all repeated bills / lines
            u_revenue = list(revenue)[0]
            yield ((invoice, stock_code, id , year) , u_revenue)

    # Returns revenues generated by customer_ids along with year,
    def reducer(self,  comb_id , revenue):
        if comb_id != None:
            invoice, stock_code, id, year = comb_id
            # Getting the unique revenue entry
            # since there are some redundant billing information repeated in both files
            u_revenue = list(revenue)[0]
            yield ((id , year) , u_revenue)

    # Total revenue generated per customer per year
    def combiner1(self, comb_id , revenue):
        yield comb_id , sum(revenue)

    # Total revenue generated per customer per year in a dictionary format
    def reducer1(self , comb_id , revenue):
        id , year = comb_id
        total_rev = sum(revenue)
        yield year, dict({id: total_rev})

    # Combines all customer_id, revenue pair for each year into a dictionary
    def combiner2(self, year, id_rev_dict):
        combined_dict = {}
        # rev_dict = Counter(combined_dict) + Counter(rev_dict)
        for rev in id_rev_dict:
        #     combined_dict = Counter(combined_dict) + Counter(rev)
            combined_dict.update(rev)

        yield year , combined_dict

    # Combines all customer_id, revenue pair for each year into a dictionary
    # Returns the top 10 customer, revenue pairs for each year
    def reducer2(self, year, id_rev_dict):
        combined_dict = {}
        for rev in id_rev_dict:
            combined_dict.update(rev)

        combined_dict = Counter(combined_dict)
        f1 = combined_dict.most_common(10)

        yield year , f1

    def steps(self):
        return [
            MRStep(mapper = self.mapper, combiner = self.combiner, reducer = self.reducer) ,
            MRStep(combiner = self.combiner1, reducer = self.reducer1),
            MRStep(combiner = self.combiner2, reducer=self.reducer2)
        ]

if __name__ == '__main__':
    MRBestCustomer.run()