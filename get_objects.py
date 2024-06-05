# list objects from s3 bucket
# Usage: python get_objects.py <bucket_name> <output_file> [prefix]
# Example: python get_objects.py my_bucket objects.txt prefix
# """
#
import os
import sys
import boto3

__doc__ = """
Usage: python get_objects.py <bucket_name> <output_file> [prefix]
Example: python get_objects.py my_bucket objects.txt prefix
"""

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    bucket_name = sys.argv[1]
    output_file = sys.argv[2]
    prefix = sys.argv[3] if len(sys.argv) > 3 else ""
    s3 = boto3.client("s3")
    try:
        os.remove(output_file)
    except OSError:
        pass
    continuation_token = None
    while True:
        if continuation_token:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuation_token, Delimiter='/')
        else:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        objects = response.get("CommonPrefixes", [])
        continuation_token = response.get("NextContinuationToken")
        with open(output_file, "a") as f:
            for obj in objects:
                f.write(obj["Prefix"] + "\n")
        print(f"{len(objects)} Objects in {bucket_name} are listed in {output_file}")
        if not continuation_token:
            break


