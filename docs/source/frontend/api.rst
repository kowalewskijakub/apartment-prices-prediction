.. _frontend_api:

Dokumentacja kluczowych funkcji (frontend)
==========================================

Poniżej znajduje się opis najważniejszych funkcji zaimplementowanych w kodzie aplikacji frontendowej.

.. module:: model_loader

.. function:: download_model_from_azure() -> TabularPredictor | None

   Pobiera, wczytuje i cachuje model predykcyjny z usługi Azure Blob Storage.

   Funkcja jest opakowana dekoratorem ``@st.cache_resource``, co oznacza, że jest wykonywana tylko raz podczas sesji aplikacji, a jej wynik jest przechowywany w pamięci podręcznej.

   Proces działania:
   1. Odczytuje poświadczenia (`AZURE_STORAGE_CONNECTION_STRING`, `AZURE_CONTAINER_NAME`) z pliku `secrets.toml`.
   2. Tworzy tymczasowy katalog lokalny.
   3. Pobiera wszystkie pliki z folderu modelu w kontenerze Azure Blob i zapisuje je w tymczasowym katalogu.
   4. Wczytuje model AutoGluon za pomocą `TabularPredictor.load()`.
   5. Wyświetla komunikat o sukcesie lub błędzie w interfejsie aplikacji.

   :returns: Obiekt `TabularPredictor` gotowy do predykcji lub `None` w przypadku błędu.


.. module:: input_utils

.. function:: get_user_inputs() -> dict

   Generuje interfejs użytkownika w pasku bocznym i zbiera dane wejściowe.

   Funkcja wykorzystuje komponenty Streamlit (np. `st.sidebar.selectbox`, `st.sidebar.number_input`) do stworzenia formularza. Wszystkie zebrane wartości są agregowane i zwracane w postaci słownika.

   :returns: Słownik, w którym klucze to nazwy cech, a wartości to dane wprowadzone przez użytkownika.


.. module:: prediction

.. function:: prepare_input_data(inputs: dict) -> pandas.DataFrame

   Przekształca surowe dane wejściowe od użytkownika w DataFrame gotowy do predykcji.

   Jest to kluczowy krok preprocessingowy, który musi być zgodny z operacjami wykonanymi na danych treningowych w backendzie.
   Kroki przetwarzania:
   1. Tworzy DataFrame z pojedynczym wierszem na podstawie słownika `inputs`.
   2. Wykonuje inżynierię cech (tworzy kolumny `age` i `floor_ratio`).
   3. Normalizuje wybrane kolumny numeryczne za pomocą funkcji `feature_utils.normalize_feature`.
   4. Dopasowuje kolumny DataFrame'u do stałej listy `MODEL_COLUMNS`, aby zapewnić pełną zgodność z modelem.

   :param inputs: Słownik z danymi z formularza.
   :returns: Jednowierszowy DataFrame gotowy do przekazania do modelu.


.. function:: make_prediction(model: TabularPredictor, input_df: pandas.DataFrame) -> float

   Wykonuje predykcję ceny na podstawie przygotowanych danych wejściowych.

   Jest to prosty wrapper na metodę `model.predict()`, który dodatkowo wyodrębnia pojedynczą wartość predykcji z wyniku.

   :param model: Wczytany obiekt modelu `TabularPredictor`.
   :param input_df: DataFrame przygotowany przez funkcję `prepare_input_data`.
   :returns: Przewidywana cena mieszkania jako liczba zmiennoprzecinkowa.


.. module:: feature_utils

.. function:: normalize_feature(value: float, feature_name: str) -> float

   Normalizuje pojedynczą wartość numeryczną przy użyciu skalowania Min-Max.

   Zakresy minimalne i maksymalne dla każdej cechy są pobierane ze słownika `NORMALIZATION_RANGES` w pliku `config.py`.

   :param value: Wartość cechy do znormalizowania.
   :param feature_name: Nazwa cechy, używana jako klucz do słownika `NORMALIZATION_RANGES`.
   :returns: Znormalizowana wartość w zakresie [0, 1].