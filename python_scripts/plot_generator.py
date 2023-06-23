import matplotlib.pyplot as plt
import numpy as np
import sys

def calculate_distance(point, centroid):
    return np.linalg.norm(point - centroid)

def assign_points_to_clusters(points, centroids):
    clusters = [[] for _ in range(len(centroids))]
    for point in points:
        distances = [calculate_distance(point, centroid) for centroid in centroids]
        closest_centroid_index = np.argmin(distances)
        clusters[closest_centroid_index].append(point)
    return clusters

def plot_clusters(clusters, num_iter, centroids):
    colors = ['red', 'blue', 'green', 'purple', 'orange', 'yellow', 'black', 'brown', 'pink', 'gray', 'olive', 'chocolate', 'sandybrown', 'aquamarine', 'lavender', 'aqua', 'navy', 'rebeccapurple', 'orchid', 'forestgreen', ]  # Add more colors if needed

    for i, (cluster_points, color) in enumerate(zip(clusters, colors)):
        cluster_points = np.array(cluster_points)
        if len(cluster_points) > 0:
            plt.scatter(cluster_points[:, 0], cluster_points[:, 1], color=color)

    centroids = np.array(centroids)
    plt.scatter(centroids[:, 0], centroids[:, 1], color='black', marker='x', s=100)  # Plot centroids as black 'x'

    plt.xlabel('X')
    plt.ylabel('Y')
    plt.title('K-means Clustering for iteration: ' + num_iter)
    plt.savefig("./plots/plot_iter" + num_iter)
    plt.close()

def parse_points_file(file_path):
    points = []
    with open(file_path, 'r') as file:
        for line in file:
            x, y = line.strip().split(',')
            points.append([float(x), float(y)])
    return np.array(points)

def parse_centroids(centroid_args):
    centroids = []
    for centroid_arg in centroid_args:
        x, y = centroid_arg.strip().split(',')
        centroids.append([float(x), float(y)])
    return np.array(centroids)

def main():
    if len(sys.argv) < 4:
        print('Usage: python kmeans.py <points_file> <iterazione> <centroid1> <centroid2> ...')
        return

    num_iter = sys.argv[1]
    centroid_args = sys.argv[2:]

    points = parse_points_file("coordinates.txt")
    centroids = parse_centroids(centroid_args)

    clusters = assign_points_to_clusters(points, centroids)
    plot_clusters(clusters, num_iter, centroids)

if __name__ == '__main__':
    main()
