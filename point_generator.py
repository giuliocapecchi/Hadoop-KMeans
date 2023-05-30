import random
import sys


def generate_coordinates(num_points, num_dimensions):
    coordinates = []
    for _ in range(num_points):
        point = []
        for _ in range(num_dimensions):
            point.append(str(random.random() * 100))  # Genera una coordinata casuale tra 0 e 1
        coordinates.append(point)
    return coordinates


def write_coordinates_to_file(filename, coordinates):
    with open(filename, 'w') as file:
        for point in coordinates:
            line = ",".join(point)  # Unisce le coordinate con una virgola
            file.write(line + '\n')  # Scrive la linea nel file con un newline alla fine


# Ottieni i parametri da riga di comando
if len(sys.argv) != 3:
    print("Utilizzo: python generate_coordinates.py <numero_punti> <numero_coordinate>")
    sys.exit(1)

try:
    num_points = int(sys.argv[1])
    num_dimensions = int(sys.argv[2])

except ValueError:
    print("Errore: I parametri 'numero_punti' e 'numero_coordinate' devono essere numeri interi.")
    sys.exit(1)

coordinates = generate_coordinates(num_points, num_dimensions)
write_coordinates_to_file("coordinates.txt", coordinates)

print(f"Il file coordinates.txt è stato generato con successo.")
