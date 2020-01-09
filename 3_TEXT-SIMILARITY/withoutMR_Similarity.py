import numpy as np
import json, random, re
import argparse
from mrjob.job import MRJob
# Gotta ask which libraries will deduce points or not? JSON is built-in and rest are for filter/functionality


def process_str2list(in_1):
    pass_1 = in_1.replace("\n", " ")
    pass_2 = re.split(r"[\.|\,|\(|\)|\s]+", pass_1)
    return pass_2[:-1]


def json_checker(fname):
    with open(fname) as file:
        try:  # load JSON file given in that directory, if fails raise Exception
            x = json.load(file)
        except ValueError:
            raise Exception('Input file is not python loadable JSON file')
    return x


def JacardCoef_1(t_a,t_b):
    # According to paper, calculation with bit vector.
    dividend = np.dot(t_a, t_b)
    divisor = np.abs(t_a)**2 + np.abs(t_b)**2 - np.dot(t_a, t_b)  # Require testing to see if subtraction works like dis
    sim = np.divide(dividend,divisor)
    return sim


def JacardCoef_2(t_a,t_b):
    # According to wikipedia intersection description, calculation by list-similarity.
    t_a = process_str2list(t_a)
    t_b = process_str2list(t_b)
    dividend = np.intersect1d(t_a, t_b)
    divisor = np.union1d(t_a, t_b)
    sim = np.divide(len(dividend),len(divisor))
    return sim

def CosineSim_1(t_a,t_b):
    # According to paper, calculation with bit vector.
    dividend = np.dot(t_a, t_b)
    divisor = np.cross(np.abs(t_a),np.abs(t_a))
    sim = np.divide(dividend,divisor)
    return sim

def main():
    max_res_pos = None
    sim = 0
    if args.jaccard is not args.cosine:
        # Jaccard Similarity Implementation part
        if args.jaccard:
            x = json_checker(args.file)
            for i in range(len(x)):
                if x[i]['id'] == "1509.02409v1":
                    art_sum = x[i]['summary']
                    index = i
            for i in range(len(x)):
                if not i == index:
                    res = JacardCoef_2(art_sum, x[i]['summary'])
                    if res >= sim:
                        max_res_pos = x[i]['id']
                        sim = res
            print(sim)
            print(max_res_pos)
        # Cosine Similarity Implementation Part
        else:
            # <------- To be done --------->
            print("Dummy until cosine is implemented")
            # <------- To be done --------->
        print("Chosen Paper Pos: " + str(index))
        print("Most Similar Paper Pos: " + str(max_res_pos))
        func = None
        if args.jaccard:
            func = "Jaccard"
        else:
            func = "Cosine"
        print("Similarity Value wrt " + func + ": " + str(sim))
    else:
        raise Exception('Either Jaccard and Cosine are both parsed as argument, or none of them was parsed!')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Text Similarity Project")
    parser.add_argument('--jaccard', action='store_true', help="Use Jaccard Similarity Coefficient?")
    parser.add_argument('--cosine', action='store_true', help="Use Cosine Similarity?")
    parser.add_argument('--file', type=str, required=True, help="Input comparison file with relative directory")
    args = parser.parse_args()

    main()