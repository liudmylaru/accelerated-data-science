import logging
import time
import sys


print("Starting the job")
argv = sys.argv[1:]

args = []
kwargs = {}

i = 0
while i < len(argv):
    if argv[i].startswith("-"):
        kwargs[argv[i]] = argv[i + 1]
        i += 2
    else:
        args.append(argv[i])
        i += 1

print(f"Positional Arguments: {args}")
print(f"Keyword Arguments: {kwargs}")

logging.getLogger().setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

print("Sleeping for 60 seconds...")
time.sleep(60)
print("After 60 seconds...")

print("Finishing the job")
