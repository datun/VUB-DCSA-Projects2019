import numpy as np


def JacardCoef_1(t_a,t_b):
    # According to paper, calculation with bit vector.
    dividend = np.dot(t_a, t_b)
    divisor = np.abs(t_a)**2 + np.abs(t_b)**2 - np.dot(t_a, t_b)  # Require testing to see if subtraction works like dis
    sim = np.divide(dividend,divisor)
    return sim


def JacardCoef_2(t_a,t_b):
    # According to wikipedia intersection description, calculation by list-similarity.
    dividend = np.intersect1d(t_a, t_b)
    divisor = np.union1d(t_a,t_b)
    sim = np.divide(dividend,divisor)
    return sim
