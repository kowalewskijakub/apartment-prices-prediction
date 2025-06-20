.. _frontend:

Aplikacja frontendowa (Streamlit)
=================================

Aplikacja webowa, zbudowana w oparciu o framework Streamlit, stanowi interfejs użytkownika (UI) dla wytrenowanego modelu predykcyjnego. Umożliwia ona w prosty i intuicyjny sposób uzyskanie prognozowanej ceny mieszkania na podstawie wprowadzonych parametrów.

Repozytorium aplikacji: `Apartment Prices Prediction App <https://github.com/kowalewskijakub/apartment-prices-prediction-app>`_

Kluczowe cechy
--------------
Kluczowe cechy aplikacji to:

*   **interaktywny formularz** – użytkownik wprowadza dane dotyczące mieszkania za pomocą wygodnych kontrolek w pasku bocznym,
*   **automatyczne ładowanie modelu** – aplikacja samodzielnie pobiera najnowszy model z Azure Blob Storage przy pierwszym uruchomieniu,
*   **przetwarzanie danych w locie** – dane wejściowe od użytkownika są przetwarzane (m.in. normalizowane i wzbogacane o nowe cechy), aby dopasować je do formatu oczekiwanego przez model,
*   **prezentacja wyników** – przewidywana cena jest wyświetlana w czytelny sposób, wraz z dodatkową informacją o cenie za metr kwadratowy.

Poniżej znajduje się szczegółowa dokumentacja poszczególnych elementów aplikacji.

.. toctree::
   :maxdepth: 1
   :caption: Dokumentacja Frontendu:

   frontend/structure
   frontend/workflow
   frontend/api