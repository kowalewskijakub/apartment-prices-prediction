.. _architecture:

Architektura systemu
====================

Architektura całego rozwiązania została podzielona na dwa główne, niezależne komponenty: **Backend** do przetwarzania danych i trenowania modelu oraz **Frontend** stanowiący interfejs użytkownika do interakcji z modelem.

Architektura ogólna
-------------------
Komponenty komunikują się ze sobą w sposób asynchroniczny poprzez usługę chmurową **Azure Blob Storage**. W ramach architektury ogólnej wyróżnia się trzy główne elementy:

1.  **backend (Kedro)** – odpowiada za cały proces ETL (Extract, Transform, Load) oraz trenowanie modelu; po zakończeniu pracy, gotowy model jest wysyłany do dedykowanego kontenera w Azure Blob Storage,
2.  **Azure Blob Storage** – pełni rolę centralnego repozytorium dla wytrenowanego artefaktu modelu,
3.  **frontend (Streamlit)** – przy uruchomieniu, aplikacja łączy się z Azure Blob Storage, pobiera najnowszą wersję modelu i ładuje ją do pamięci, aby być gotową do serwowania predykcji.

Taka architektura zapewnia separację zadań (separation of concerns), umożliwiając niezależny rozwój i skalowanie obu części systemu.

Architektura Backendu (Kedro)
-----------------------------
Projekt oparty jest na frameworku **Kedro**, który narzuca modularną i skalowalną strukturę. Kluczowe elementy architektury to:

*   **potoki (pipelines)** – logika projektu jest zorganizowana w postaci potoków przetwarzania danych; Każdy potok składa się z węzłów (nodes), które realizują określone zadania,
*   **katalog danych (data catalog)** – Kedro zarządza ładowaniem i zapisywaniem danych w sposób abstrakcyjny, co ułatwia pracę z różnymi źródłami danych,
*   **konfiguracja** – parametry działania potoków oraz dane uwierzytelniające są zarządzane poprzez pliki konfiguracyjne, co oddziela logikę od konfiguracji.

Architektura frontendu (Streamlit)
----------------------------------
Aplikacja frontendowa jest zbudowana przy użyciu **Streamlit**. Jej architektura jest komponentowa i opiera się na kilku kluczowych modułach:

*   **główny skrypt aplikacji (`app.py`)** – orkiestruje cały przepływ, od załadowania modelu, przez zebranie danych od użytkownika, po wyświetlenie wyniku predykcji,
*   **moduł ładowania modelu (`model_loader.py`)** – izoluje logikę odpowiedzialną za pobieranie i cachowanie modelu z chmury,
*   **moduły pomocnicze (`input_utils.py`, `feature_utils.py`, `prediction.py`)** – zawierają funkcje odpowiedzialne za renderowanie interfejsu, przygotowanie danych wejściowych i dokonywanie predykcji.

Technologie
-----------
*   **Backend**:
    *   **Kedro**: Framework do budowy potoków danych.
    *   **Pandas**: Biblioteka do manipulacji danymi.
    *   **Scikit-learn**: Biblioteka do zadań pomocniczych w uczeniu maszynowym.
    *   **AutoGluon**: Zautomatyzowana platforma uczenia maszynowego (AutoML).
*   **Frontend**:
    *   **Streamlit**: Framework do tworzenia interaktywnych aplikacji webowych.
*   **Infrastruktura**:
    *   **Azure Blob Storage**: Usługa chmurowa do przechowywania wytrenowanego modelu.