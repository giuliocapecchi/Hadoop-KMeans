import random

def generate_coordinates(num_points, num_dimensions):
    coordinates = []
    for _ in range(num_points):
        point = []
        for _ in range(num_dimensions):
            point.append(str(random.random()))  # Genera una coordinata casuale tra 0 e 1
        coordinates.append(point)
    return coordinates

def write_coordinates_to_file(filename, coordinates):
    with open(filename, 'w') as file:
        for point in coordinates:
            line = ",".join(point)  # Unisce le coordinate con una virgola
            file.write(line + '\n')  # Scrive la linea nel file con un newline alla fine

num_points = 10  # Numero di punti
num_dimensions = 3  # Numero di dimensioni (coordinate) per ogni punto
filename = 'coordinates.txt'

coordinates = generate_coordinates(num_points, num_dimensions)
write_coordinates_to_file(filename, coordinates)

print(f"Il file {filename} è stato generato con successo.")