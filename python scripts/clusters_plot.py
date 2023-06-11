import matplotlib.pyplot as plt

clusters = {}
current_cluster = None

# Leggi il file di testo
with open("../output/clusters.txt", "r") as file:
    lines = file.readlines()

# Elabora le righe del file
for line in lines:
    line = line.strip()
    if line.startswith("Chiave:"):
        current_cluster = int(line.split(":")[1])
        clusters[current_cluster] = []
    else:
        point = eval(line)
        clusters[current_cluster].append(point)

# Rappresenta i punti nel piano con colori diversi per ogni cluster
for cluster, points in clusters.items():
    x = [point[0] for point in points]
    y = [point[1] for point in points]
    plt.scatter(x, y, label=f"Cluster {cluster}")

# Imposta le etichette degli assi e la legenda
plt.xlabel("X")
plt.ylabel("Y")
plt.legend()

# Mostra il grafico
plt.show()
