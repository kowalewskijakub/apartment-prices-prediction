.. _installation:

Instalacja i konfiguracja
=========================

Poniższa instrukcja opisuje kroki niezbędne do uruchomienia zarówno środowiska backendowego (Kedro), jak i aplikacji frontendowej (Streamlit).

Wymagania wstępne
-----------------
*   Python 3.9+
*   Git

Instalacja Backendu (Kedro)
---------------------------
1.  **Sklonuj repozytorium projektu:**

    .. code-block:: bash

       git clone https://github.com/kowalewskijakub/apartment-prices-prediction.git
       cd apartment-prices-prediction

2.  **Stwórz i aktywuj wirtualne środowisko (zalecane):**

    .. code-block:: bash

       python -m venv venv
       source venv/bin/activate  # na systemach Windows: venv\Scripts\activate

3.  **Zainstaluj zależności:**

    .. code-block:: bash

       pip install -r requirements.txt

4.  **Konfiguracja Azure (opcjonalnie):**
    Aby potok `azure_upload` działał poprawnie, należy ustawić zmienną środowiskową `AZURE_STORAGE_CONNECTION_STRING` z kluczem dostępu do konta Azure Storage.

    .. code-block:: bash

       export AZURE_STORAGE_CONNECTION_STRING="<Your-Connection-String>"

5.  **Uruchomienie domyślnego potoku:**

    .. code-block:: bash

       kedro run

Instalacja Frontendu (Streamlit)
--------------------------------
1.  **Sklonuj repozytorium aplikacji:**

    .. code-block:: bash

       git clone https://github.com/kowalewskijakub/apartment-prices-prediction-app.git
       cd apartment-prices-prediction-app

2.  **Stwórz i aktywuj wirtualne środowisko (zalecane):**

    .. code-block:: bash

       python -m venv venv
       source venv/bin/activate  # na systemach Windows: venv\Scripts\activate

3.  **Zainstaluj zależności:**

    .. code-block:: bash

       pip install -r requirements.txt

4.  **Konfiguracja poświadczeń Azure:**
    Aplikacja Streamlit wymaga dostępu do Azure Blob Storage w celu pobrania modelu. Stwórz plik `.streamlit/secrets.toml` i uzupełnij go swoimi danymi:

    .. code-block:: toml

       # .streamlit/secrets.toml
       AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=..."
       AZURE_CONTAINER_NAME = "nazwa-twojego-kontenera"

5.  **Uruchomienie aplikacji:**

    .. code-block:: bash

       streamlit run app.py