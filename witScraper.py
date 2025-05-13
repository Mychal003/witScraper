# Import niezbędnych bibliotek
import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL strony z harmonogramem egzaminów
url = "https://wit.pwr.edu.pl/studenci/organizacja-toku-studiow/harmonogram-egzaminow"

# Pobranie zawartości strony
try:
    response = requests.get(url)
    response.raise_for_status()  # Sprawdzenie błędów HTTP
    html = response.content
except Exception as e:
    print(f"Wystąpił błąd podczas pobierania strony: {e}")
    exit()

# Parsowanie HTML
soup = BeautifulSoup(html, "html.parser")

# Znalezienie wszystkich tabel na stronie
tables = soup.find_all("table")

# Lista do przechowywania danych o egzaminach
exam_data = []

# Iteracja przez wszystkie tabele
for table in tables:
    # Pobranie wszystkich wierszy tabeli
    rows = table.find_all("tr")
    
    # Sprawdzenie, czy tabela ma wystarczającą liczbę wierszy
    if len(rows) < 2:
        continue
    
    # Pobranie nagłówków tabeli
    header_row = rows[0]
    headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]
    
    if not ("Termin" in headers and "data" in headers and "godzina" in headers):
        continue
    
    # Zmienna do przechowywania danych o bieżącym kursie
    current_course = None
    
    # Iteracja przez wiersze danych (pomijając nagłówek)
    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        row_data = [cell.get_text(strip=True) for cell in cells]
        
        # Ignorowanie pustych wierszy
        #if not row_data or all(not cell for cell in row_data):
            #continue
        
        # Sprawdzenie, czy to wiersz z pełnymi danymi kursu
        if len(row_data) >= 5 and "II termin" not in row_data:
            # Tworzenie słownika z danymi wiersza
            course_data = {}
            
            # Mapowanie danych do odpowiednich pól
            for i, header in enumerate(headers):
                if i < len(row_data):
                    course_data[header] = row_data[i]
            
            # Zapisanie bieżącego kursu
            current_course = course_data
            
            # Dodanie danych dla pierwszego terminu
            exam_data.append(current_course)
        
        # Sprawdzenie, czy to wiersz z informacją o drugim terminie
        elif "II termin" in row_data and current_course:
            # Tworzenie kopii danych bieżącego kursu
            second_term = current_course.copy()
            
            # Indeks dla "II termin" w wierszu
            termin_index = row_data.index("II termin")
            
            # Aktualizacja terminu
            second_term["Termin"] = "II termin"
            
            # Aktualizacja pozostałych pól dla drugiego terminu
            fields = ["data", "godzina", "sala", "budynek"]
            for i, field in enumerate(fields):
                if field in headers and termin_index + 1 + i < len(row_data):
                    second_term[field] = row_data[termin_index + 1 + i]
            
            # Dodanie danych dla drugiego terminu
            exam_data.append(second_term)

# Konwersja listy słowników na DataFrame
df = pd.DataFrame(exam_data)

# Zmiana nazw kolumn na bardziej czytelne
column_mapping = {
    "Kod kursu": "Kod kursu",
    "Nazwa kursu": "Nazwa kursu",
    "Prowadzący": "Prowadzący",
    "Termin": "Termin",
    "data": "Data",
    "godzina": "Godzina",
    "sala": "Sala",
    "budynek": "Budynek"
}
df = df.rename(columns=column_mapping)

# Zapisanie danych do pliku CSV
df.to_csv("harmonogram_egzaminow.csv", index=False, encoding="utf-8")
print("Dane zostały zapisane do pliku harmonogram_egzaminow.csv")
