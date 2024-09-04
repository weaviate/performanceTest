import matplotlib.pyplot as plt
import re

def read_qps_from_log(log_file_path, pattern):
    """
    Reads QPS values from a log file based on a specific pattern.

    :param log_file_path: Path to the log file
    :param pattern: Regular expression pattern to match and extract the QPS value
    :return: A list of extracted QPS values
    """

    qps_values = []

    with open(log_file_path, 'r') as file:
        for line in file:
            match = re.search(pattern, line)
            if match:
                qps_values.append(float(match.group(1)))

    return qps_values

def plot_qps_comparison(qps_values_1, qps_values_2, index_name_1, index_name_2):
    """
    Plots a comparison of QPS values from two vector indices.

    :param qps_values_1: QPS values for the hnsw index
    :param qps_values_2: QPS values for the flat index
    :param qps_values_3: QPS values for the dynamic index
    :param index_name_1: Name of the first index
    :param index_name_2: Name of the second index
    """
    plt.figure(figsize=(10, 5))

    plt.plot(qps_values_1, label=index_name_1, marker='o')
    plt.plot(qps_values_2, label=index_name_2, marker='o')

    plt.xlabel('Tenants (Objects stored in a tennant)')
    plt.ylabel('QPS (Queries Per Second)')
    plt.title('QPS Comparison Between hnsw , dynamic vector index type')
    plt.legend()
    plt.grid(True)
    plt.show()

# Define log file paths for both indices
log_file_path_1 = './hnsw.log'
log_file_path_2 = './dynamic.log'
log_file_path_3 = './flat.log'

# Define the pattern to extract QPS values
# Assuming the format 'QPS: <value>'
qps_pattern = r'qps :\s*([\d.]+)'

# Read QPS values from both log files
qps_values_hnsw = read_qps_from_log(log_file_path_1, qps_pattern)
qps_values_dynamic = read_qps_from_log(log_file_path_2, qps_pattern)

# Plot the QPS comparison
plot_qps_comparison(qps_values_hnsw, qps_values_dynamic, 'HNSW Vector index', 'Dynamic Vector index')
