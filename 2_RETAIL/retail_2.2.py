from mrjob.job import MRJob
from mrjob.step import MRStep


class MRBestProduct(MRJob):
    # pre-processing
    def mapper(self, _, line):
    # Removes the first line in csv
    # Because all the below operations cannot be performed with the header
        try:
            pre_process = line.split(',')
            # The description already have commas.
            # So the fields need to be extracted from the right
            length = len(pre_process)
            invoice = pre_process[0]
            stock_code = pre_process[1]
            quantity = float(pre_process[length - 5])
            revenue = float(pre_process[length - 3]) * quantity
            yield ((invoice, stock_code), (quantity, revenue))

        except:
            yield (None, 0)

    def combiner(self, comb_id, revenue_quan):
        if comb_id != None:
            invoice, stock_code= comb_id
    #         # Discarding all repeated bills / lines
            u_quan_revenue = list(revenue_quan)[0]
            yield (invoice, stock_code), u_quan_revenue


    # Returns revenues and quantities  generated for products
    def reducer(self, comb_id, quan_revenue):
        if comb_id != None:
            invoice, stock_code = comb_id
            # Getting the unique revenue entry / Discarding all repeated bills
            # since there are some redundant billing information repeated in both files
            u_quan_revenue = list(quan_revenue)[0]
            yield stock_code, u_quan_revenue

    # The following combiner and reducer calculates the sum of
    # Quantity and Revenue per product
    def combiner1(self, prod_id, quan_rev):
        if prod_id != None:
            sum_q , sum_r = 0, 0
            for q, r in quan_rev:
                sum_q += q
                sum_r += r
            yield  prod_id, (sum_q, sum_r)

    def reducer1(self, prod_id, quan_rev):
        if prod_id != None:
            sum_q, sum_r = 0, 0
            for q, r in quan_rev:
                sum_q += q
                sum_r += r
            yield None, (sum_q, sum_r, prod_id)

    # Finds products with max revenue and quantities
    def reducer2(self, _, quan_rev_sums):
        max_rev_prod = ""
        max_quan_prod = ""
        max_rev = 0
        max_quan = 0
        for sq, sr, p in quan_rev_sums:
            if sr > max_rev:
                max_rev = sr
                max_rev_prod = p
            # if there are multiple products with the same total revenue generated
            elif sr == max_rev:
                max_rev_prod += p

            if sq > max_quan:
                max_quan = sq
                max_quan_prod = p
                # if there are multiple products with the same total revenue generated
            elif sq == max_quan:
                max_quan_prod += p

        # The following line return the product id and the quantity/revenue
        # The key is provided as a readable text
        keys = ['Max Revenue', 'Max Quantity']
        return_val = list([[max_rev_prod, max_rev], [max_quan_prod, max_quan]])
        for i, key in enumerate(keys):
            yield key , return_val[i]

    def steps(self):
        return [
            MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
            MRStep(combiner=self.combiner1, reducer = self.reducer1),
            MRStep( reducer = self.reducer2)
        ]

if __name__ == '__main__':
    MRBestProduct.run()