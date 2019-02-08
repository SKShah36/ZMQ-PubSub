import csv
import matplotlib.pyplot as plt


def csvread(filename):
    with open(filename) as csv_file:
        count = []
        time_difference = []
        average_latency = []
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count = 1
            else:
                count.append(int(row[0]))
                time_difference.append(float(row[1]))
                average_latency.append(float(row[2]))

    return count, time_difference, average_latency


count, time_difference, average_latency = csvread("latency_data_1x10-20210.csv")

plt.figure()
plt.plot(count, average_latency, 'r')
plt.xlabel("Count")
plt.ylabel("Average latency")
plt.show()

