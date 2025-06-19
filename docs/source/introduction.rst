.. _introduction:

Wprowadzenie
============

Projekt **Apartment Prices Prediction** został zrealizowany w celu opracowania zaawansowanego modelu predykcyjnego, zdolnego do szacowania cen mieszkań w oparciu o ich atrybuty. Wykorzystuje on nowoczesne technologie i metodyki z zakresu inżynierii danych i uczenia maszynowego.

Architektura projektu
---------------------

Projekt oparty jest na frameworku **Kedro**, który narzuca modularną i skalowalną strukturę. Kluczowe elementy architektury to:
*   **potoki (pipelines)** – logika projektu jest zorganizowana w postaci potoków przetwarzania danych. Każdy potok składa się z węzłów (nodes), które realizują określone zadania,
*   **katalog danych (data catalog)** – kedro zarządza ładowaniem i zapisywaniem danych w sposób abstrakcyjny, co ułatwia pracę z różnymi źródłami danych,
*   **konfiguracja** – parametry działania potoków oraz dane uwierzytelniające są zarządzane poprzez pliki konfiguracyjne, co oddziela logikę od konfiguracji.

Technologie
-----------

Główne technologie wykorzystane w projekcie to:
*   **Kedro** – framework do budowy potoków danych,
*   **Pandas** – biblioteka do manipulacji danymi,
*   **Scikit-learn** – biblioteka do zadań pomocniczych w uczeniu maszynowym (imputacja, skalowanie),
*   **AutoGluon** – zautomatyzowana platforma uczenia maszynowego (AutoML) do trenowania wysokiej jakości modeli predykcyjnych,
*   **Azure Blob Storage** – usługa chmurowa do przechowywania wytrenowanego modelu.

Przepływ pracy
--------------

Domyślny przepływ pracy w projekcie obejmuje trzy główne etapy, realizowane przez odrębne potoki:

1.  **Przetwarzanie danych (`data_processing`)** – surowe dane są łączone, czyszczone, transformowane i przygotowywane do modelowania,
2.  **Trenowanie modelu (`autogluon_model`)** – przygotowane dane są wykorzystywane do trenowania modelu predykcyjnego przy użyciu biblioteki AutoGluon,
3.  **Wysyłanie modelu do chmury (`azure_upload`)** – wytrenowany model jest archiwizowany i wysyłany do kontenera w usłudze Azure Blob Storage w celu dalszego wykorzystania.