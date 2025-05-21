import pandas as pd
import csv
from py2neo import Graph, Node, Relationship
import re

# Funkcja do podziału kierunków/specjalności
def split_directions(directions_str):
    if pd.isna(directions_str):
        return []
    return [direction.strip() for direction in re.split(r',\s*', directions_str)]

# Funkcja do podziału kodów kursów
def split_course_codes(codes_str):
    if pd.isna(codes_str):
        return []
    # Usunięcie cudzysłowów jeśli są obecne
    codes_str = codes_str.strip('"')
    # Podział na oddzielne kody kursów
    return [code.strip() for code in re.split(r',\s*', codes_str)]

# Funkcja do podziału prowadzących
def split_teachers(teachers_str):
    if pd.isna(teachers_str):
        return []
    teachers = []
    # Usunięcie cudzysłowów jeśli są obecne
    teachers_str = teachers_str.strip('"')
    # Podział na oddzielnych prowadzących
    for teacher in re.split(r',\s*', teachers_str):
        teachers.append(teacher.strip())
    return teachers

# Funkcja do podziału sal
def split_rooms(rooms_str):
    if pd.isna(rooms_str):
        return []
    # Usunięcie cudzysłowów jeśli są obecne
    rooms_str = rooms_str.strip('"')
    # Podział na oddzielne sale
    return [room.strip() for room in re.split(r',\s*', rooms_str)]

# Funkcja do podziału budynków
def split_buildings(buildings_str):
    if pd.isna(buildings_str):
        return []
    # Usunięcie cudzysłowów jeśli są obecne
    buildings_str = buildings_str.strip('"')
    # Podział na oddzielne budynki
    return [building.strip() for building in re.split(r',\s*', buildings_str)]

# Funkcja do wczytania danych CSV
def load_csv_data(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            data.append(row)
    return data

# Funkcja do tworzenia grafu w Neo4j
def create_knowledge_graph(data):
    # Połączenie z Neo4j - dostosuj URI, użytkownika i hasło do swojej instancji
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "testpassword"))
    
    # Wyczyszczenie bazy danych (opcjonalne)
    graph.run("MATCH (n) DETACH DELETE n")
    
    # Utworzenie indeksów dla szybszego wyszukiwania
    graph.run("CREATE INDEX IF NOT EXISTS FOR (c:Course) ON (c.name)")
    graph.run("CREATE INDEX IF NOT EXISTS FOR (t:Teacher) ON (t.name)")
    graph.run("CREATE INDEX IF NOT EXISTS FOR (d:Direction) ON (d.name)")
    graph.run("CREATE INDEX IF NOT EXISTS FOR (r:Room) ON (r.number)")
    graph.run("CREATE INDEX IF NOT EXISTS FOR (b:Building) ON (b.code)")
    graph.run("CREATE INDEX IF NOT EXISTS FOR (e:Exam) ON (e.date, e.time)")
    
    # Słowniki do śledzenia utworzonych węzłów
    courses = {}
    teachers = {}
    directions = {}
    rooms = {}
    buildings = {}
    
    # Przetwarzanie danych
    for row in data:
        # Przetwarzanie kursu
        course_name = row['Nazwa kursu']
        if course_name not in courses:
            course_node = Node("Course", name=course_name)
            graph.create(course_node)
            courses[course_name] = course_node
        else:
            course_node = courses[course_name]
            
        # Przetwarzanie kodów kursu
        for code in split_course_codes(row['Kod kursu']):
            course_node['code'] = code  # Przypisujemy kod do kursu
            
        # Przetwarzanie prowadzących
        for teacher_name in split_teachers(row['Prowadzący']):
            if teacher_name not in teachers:
                teacher_node = Node("Teacher", name=teacher_name)
                graph.create(teacher_node)
                teachers[teacher_name] = teacher_node
            else:
                teacher_node = teachers[teacher_name]
                
            # Relacja między prowadzącym a kursem
            teaches_rel = Relationship(teacher_node, "TEACHES", course_node)
            graph.create(teaches_rel)
            
        # Przetwarzanie kierunków/specjalności
        for direction_name in split_directions(row['Kierunek/Specjalność']):
            if direction_name not in directions:
                direction_node = Node("Direction", name=direction_name)
                graph.create(direction_node)
                directions[direction_name] = direction_node
            else:
                direction_node = directions[direction_name]
                
            # Relacja między kierunkiem a kursem
            has_course_rel = Relationship(direction_node, "HAS_COURSE", course_node)
            graph.create(has_course_rel)
            
        # Przetwarzanie egzaminu
        exam_term = row['Termin']
        exam_date = row['Data']
        exam_time = row['Godzina']
        
        exam_node = Node("Exam", 
                        term=exam_term, 
                        date=exam_date, 
                        time=exam_time)
        graph.create(exam_node)
        
        # Relacja między kursem a egzaminem
        has_exam_rel = Relationship(course_node, "HAS_EXAM", exam_node)
        graph.create(has_exam_rel)
        
        # Przetwarzanie sal
        room_list = split_rooms(row['Sala'])
        building_list = split_buildings(row['Budynek'])
        
        # Jeśli jest więcej budynków niż sal, używamy tylko dostępnych sal
        # W przeciwnym razie przetwarzamy pary sala-budynek
        for i, room_number in enumerate(room_list):
            if room_number and room_number != "":  # Sprawdzenie czy sala nie jest pusta
                if room_number not in rooms:
                    room_node = Node("Room", number=room_number)
                    graph.create(room_node)
                    rooms[room_number] = room_node
                else:
                    room_node = rooms[room_number]
                
                # Relacja między egzaminem a salą
                in_room_rel = Relationship(exam_node, "IN_ROOM", room_node)
                graph.create(in_room_rel)
                
                # Przetwarzanie budynku tylko jeśli mamy odpowiadający indeks w liście budynków
                if i < len(building_list) and building_list[i] and building_list[i] != "":
                    building_code = building_list[i]
                    if building_code not in buildings:
                        building_node = Node("Building", code=building_code)
                        graph.create(building_node)
                        buildings[building_code] = building_node
                    else:
                        building_node = buildings[building_code]
                    
                    # Relacja między salą a budynkiem
                    in_building_rel = Relationship(room_node, "IN_BUILDING", building_node)
                    graph.create(in_building_rel)
    
    print("Graf wiedzy został utworzony w Neo4j!")

# Główna funkcja
def main():
    file_path = 'harmonogram_egzaminow.csv'  # Podaj ścieżkę do swojego pliku CSV
    
    try:
        # Wczytanie danych z CSV do DataFrame
        df = pd.read_csv(file_path)
        print(f"Wczytano dane: {len(df)} wierszy")
        
        # Konwersja DataFrame do listy słowników
        data = df.to_dict('records')
        
        # Tworzenie grafu wiedzy
        create_knowledge_graph(data)
        
    except Exception as e:
        print(f"Wystąpił błąd: {e}")

if __name__ == "__main__":
    main()