.. _introduction:

Wprowadzenie
============

Projekt **Apartment Prices Prediction** został zrealizowany w celu opracowania zaawansowanego modelu predykcyjnego, zdolnego do szacowania cen mieszkań w oparciu o ich atrybuty. Wykorzystuje on nowoczesne technologie i metodyki z zakresu inżynierii danych i uczenia maszynowego.

Aplikacja jest utrzymywana w następujących repozytoriach GitHub:

*   `Apartment Prices Prediction <https://github.com/kowalewskijakub/apartment-prices-prediction>`_ – repozytorium główne projektu, zawierające kod źródłowy modelu predykcyjnego oraz potoki przetwarzania danych (backend).
*   `Apartment Prices Prediction App <https://github.com/kowalewskijakub/apartment-prices-prediction-app>`_ – repozytorium aplikacji webowej opartej na Streamlit, która umożliwia interakcję z modelem predykcyjnym (frontend).

Szczegółowy opis architektury obu komponentów oraz użytych technologii znajduje się w sekcji :doc:`architecture`.

Domyślny przepływ pracy w projekcie backendowym obejmuje trzy główne etapy, realizowane przez odrębne potoki:

1.  **przetwarzanie danych** – surowe dane są łączone, czyszczone, transformowane i przygotowywane do modelowania,
2.  **trenowanie modelu** – przygotowane dane są wykorzystywane do trenowania modelu predykcyjnego przy użyciu biblioteki AutoGluon,
3.  **wysyłanie modelu do chmury** – wytrenowany model jest archiwizowany i wysyłany do kontenera w usłudze Azure Blob Storage w celu dalszego wykorzystania przez aplikację frontendową.

Działanie aplikacji frontendowej zostało szczegółowo opisane w rozdziale :doc:`frontend`.