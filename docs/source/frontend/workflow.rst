.. _frontend_workflow:

Przepływ pracy w aplikacji
==========================

Logika aplikacji frontendowej realizuje sekwencyjny proces, który rozpoczyna się od jej uruchomienia, a kończy na wyświetleniu predykcji.

Szczegółowy przepływ
--------------------

1.  **Uruchomienie i ładowanie modelu**
    *   Użytkownik uruchamia aplikację poleceniem `streamlit run app.py`.
    *   Skrypt `app.py` natychmiast wywołuje funkcję `model_loader.download_model_from_azure()`.
    *   Dzięki dekoratorowi `@st.cache_resource`, funkcja ta jest wykonywana tylko raz na sesję.
    *   Nawiązywane jest połączenie z Azure Blob Storage przy użyciu poświadczeń z `st.secrets`.
    *   Cały katalog z modelem AutoGluon jest pobierany do tymczasowego folderu na lokalnym dysku.
    *   Model jest ładowany do pamięci za pomocą `TabularPredictor.load()` i zwracany do głównego skryptu.

2.  **Interakcja z użytkownikiem**
    *   Funkcja `input_utils.get_user_inputs()` renderuje w pasku bocznym interaktywny formularz z polami do wprowadzenia cech mieszkania.
    *   Użytkownik wypełnia formularz, a Streamlit na bieżąco przechowuje wprowadzone wartości.

3.  **Inicjacja predykcji**
    *   Użytkownik klika przycisk "Predict Price".
    *   Warunek `if st.button(...)` zostaje spełniony, co uruchamia logikę predykcji.

4.  **Przygotowanie danych wejściowych**
    *   Słownik z danymi od użytkownika jest przekazywany do funkcji `prediction.prepare_input_data()`.
    *   Wewnątrz tej funkcji wykonywane są następujące kroki:
        a.  Dane są konwertowane na DataFrame biblioteki Pandas.
        b.  Tworzone są nowe cechy, takie jak `age` (wiek budynku) i `floor_ratio` (stosunek piętra do liczby pięter).
        c.  Wartości numeryczne są normalizowane za pomocą skalowania Min-Max, przy użyciu zakresów zdefiniowanych w `config.py`.
        d.  Struktura DataFrame'u jest finalizowana, aby zapewnić, że zawiera dokładnie te same kolumny, których oczekuje model.

5.  **Generowanie predykcji**
    *   Przygotowany i sformatowany DataFrame jest przekazywany do funkcji `prediction.make_prediction()`.
    *   Funkcja ta wywołuje metodę `model.predict()` na danych wejściowych.
    *   Model AutoGluon zwraca przewidywaną cenę.

6.  **Wyświetlanie wyników**
    *   Wynik predykcji jest formatowany i wyświetlany w głównej części aplikacji w postaci dużej, czytelnej liczby.
    *   Dodatkowo obliczana i wyświetlana jest przewidywana cena za metr kwadratowy, aby dać użytkownikowi dodatkowy kontekst.