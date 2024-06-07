import os
import sys
import uuid

import boto3

__doc__ = """
Usage: python test_generate.py <num_of_linex> <output_file>
Example: python get_objects.py 15000 objects.txt 
"""

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    num_of_lines = int(sys.argv[1])
    output_file = sys.argv[2]

    one_of_10 = num_of_lines/10
    one_of_100 = num_of_lines/100
    one_of_1000 = num_of_lines/1000
    with (open(output_file, "w") as output_file_handle, open(output_file+"test10", "w") as test_10_file_handle,
          open(output_file+"test100", "w") as test_100_file_handle, open(output_file+"test1000", "w")
          as test_1000_file_handle
          ):
        for i in range(num_of_lines):
            id = uuid.uuid4()
            output_file_handle.write(f"{id}\n")
            test_id = str(id)
            if i%7 == 0:
                test_id = test_id + "test"
            if i % one_of_10 == 0:
                test_10_file_handle.write(f"{test_id}\n")
            if i % one_of_100 == 0:
                test_100_file_handle.write(f"{test_id}\n")
            if i % one_of_1000 == 0:
                    test_1000_file_handle.write(f"{test_id}\n")




