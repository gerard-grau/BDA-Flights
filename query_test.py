import time
from dw import DW
import extract


def time_and_print(function):
    start = time.perf_counter()
    result = function()
    end = time.perf_counter()
    print(result)
    print(f"Execution time: {end - start:.4f} seconds")


if __name__ == '__main__':
    dw = DW(create=False)
    print("\n*************************************************** Query Aircraft Utilization")
    print("================================ DW ======================================")
    # time_and_print(dw.query_utilization)
    print("============================= Baseline ===================================")
    # time_and_print(extract.query_utilization_baseline)
    print("\n************************************************************* Query Reporting")
    print("================================ DW ======================================")
    # time_and_print(dw.query_reporting)
    print("============================= Baseline ===================================")
    time_and_print(extract.query_reporting_baseline)
    print("\n***************************************************** Query Reporting per Role")
    print("================================ DW ======================================")
    # time_and_print(dw.query_reporting_per_role)
    print("============================= Baseline ===================================")
    time_and_print(extract.query_reporting_per_role_baseline)
    dw.close()

