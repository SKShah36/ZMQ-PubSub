import csv
import matplotlib.pyplot as plt
import os


def count_vs_latency(pub, sub):
    plt.figure()
    plt.plot(our_message_count, our_average_latency_list, 'r')
    plt.title("Publisher to Subscriber - {}x{}".format(pub, sub))
    plt.xlabel("Message Count")
    plt.ylabel("Average latency")
    plt.savefig("{}/{}x{}".format(directory, pub, sub))
    plt.show()


def csvread(filename):
    with open(filename) as csv_file:
        message_count = []
        time_difference = []
        average_latency = []

        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count = 1

            else:
                message_count.append(int(row[0]))
                time_difference.append(float(row[1]))
                average_latency.append(float(row[2]))

            if len(message_count) == 400:
                break

    return message_count, average_latency[len(average_latency)-1], average_latency


directory = "{}/Performance_Log/latency_data_10x1".format(os.getcwd())
csv_files = []
for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        csv_files.append(os.path.join(directory, filename))
        continue
    else:
        continue

latency = dict()
# count = []

for files in csv_files:
    message_count, average_latency, average_latency_list = csvread("{}".format(files))
    latency[average_latency] = (message_count, average_latency_list)
    print(average_latency)


max_latency = max(latency.keys())
our_message_count = latency[max_latency][0]
our_average_latency_list = latency[max_latency][1]
count_vs_latency(10, 1)




