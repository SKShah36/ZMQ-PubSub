import subprocess
import sys
import time

number_processes = 100
process_name = "subscriber"

if len(sys.argv) > 1:
    process_name = sys.argv[1]

if len(sys.argv) > 2:
    number_processes = int(sys.argv[2])

procs = []

for i in range(number_processes):
    proc = subprocess.Popen(["python3", "{}.py".format(process_name)])
    procs.append(proc)
    time.sleep(1)

for proc in procs:
    proc.wait()