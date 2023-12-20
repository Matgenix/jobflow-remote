from random import randint

from jobflow import Flow, Response, job


@job
def add(a, b):
    """
    A Job adding to numbers
    """
    return a + b


@job
def add_many(*to_sum):
    """
    A job adding numbers
    """
    return sum(to_sum)


@job
def sleep(s):
    """
    A Job sleeping.
    """
    import time

    time.sleep(s)
    return s


@job
def add_raise(a, b):
    """
    A Job raising a RuntimeError
    """
    raise RuntimeError("An error for a and b")


@job
def make_list(a, length=None):
    """
    A Job generating a list of numbers
    """
    if not length:
        length = randint(2, 5)
    return [a] * length


@job
def add_distributed(list_a):
    """
    A Job generating a new Flow to add a list of numbers
    """
    jobs = []
    for val in list_a:
        jobs.append(add(val, 1))

    flow = Flow(jobs)
    return Response(replace=flow)


@job
def value(n):
    """
    A Job returning the input value
    """
    return n


@job
def conditional_sum_replace(numbers, min_n=10):
    """
    A Job creating a replace and adding number until a value is reached.
    """
    s = sum(numbers)
    if s < min_n:
        j1 = value(s)
        j2 = value(5)
        c = conditional_sum_replace([j1.output, j2.output], min_n=min_n)
        return Response(replace=Flow([j1, j2, c], output=c.output))
    else:
        return s
