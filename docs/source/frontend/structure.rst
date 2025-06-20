.. _frontend_structure:

Struktura plików aplikacji
==========================

Kod aplikacji frontendowej jest zorganizowany w kilku modułach, z których każdy ma jasno zdefiniowaną odpowiedzialność. Taka struktura ułatwia zarządzanie kodem i jego dalszy rozwój.

Główne pliki projektu
---------------------

*   **`app.py`**
    Główny plik aplikacji, odpowiedzialny za jej uruchomienie i orkiestrację. Inicjuje interfejs Streamlit, wywołuje funkcje do ładowania modelu i pobierania danych od użytkownika, a także koordynuje proces predykcji i wyświetlania wyników.

*   **`model_loader.py`**
    Zawiera logikę związaną z pobieraniem modelu predykcyjnego z chmury Azure Blob Storage. Funkcja `download_model_from_azure` jest cachowana (`@st.cache_resource`), aby uniknąć wielokrotnego pobierania modelu podczas interakcji z aplikacją.

*   **`input_utils.py`**
    Moduł odpowiedzialny za tworzenie interfejsu użytkownika. Funkcja `get_user_inputs` generuje wszystkie widżety (pola wyboru, suwaki, pola tekstowe) w pasku bocznym Streamlit i zbiera dane wprowadzone przez użytkownika do słownika.

*   **`prediction.py`**
    Centralny moduł logiki predykcyjnej. Zawiera dwie kluczowe funkcje:
    *   `prepare_input_data`: Przekształca surowy słownik danych od użytkownika w sformatowany DataFrame, wykonując inżynierię cech i normalizację.
    *   `make_prediction`: Wykonuje właściwą predykcję na przygotowanych danych.

*   **`feature_utils.py`**
    Moduł pomocniczy zawierający funkcje do przetwarzania cech. Obecnie znajduje się tu funkcja `normalize_feature`, która skaluje wartości numeryczne.

*   **`config.py`**
    Plik konfiguracyjny, w którym przechowywane są stałe wartości, takie jak `NORMALIZATION_RANGES`. Centralizuje to parametry, które muszą być spójne z procesem trenowania modelu w backendzie.