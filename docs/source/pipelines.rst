.. _pipelines:

Architektura potoków (pipelines)
================================

Logika biznesowa projektu jest zorganizowana w modularne i reużywalne potoki (pipelines), zgodnie z metodyką Kedro. Każdy potok reprezentuje logiczny etap przetwarzania danych.

Główne potoki w projekcie
-------------------------

Projekt definiuje trzy główne potoki, które są rejestrowane w pliku `pipeline_registry.py`:

1.  **data_processing** – odpowiedzialny za pełne przygotowanie danych – od surowych, połączonych plików do oczyszczonego i przetworzonego zbioru gotowego do modelowania,
2.  **autogluon_model** – odpowiedzialny za proces trenowania modelu uczenia maszynowego – dzieli dane na zbiory treningowe i testowe, trenuje model przy użyciu AutoGluon, a następnie generuje predykcje na zbiorze testowym,
3.  **azure_upload** – odpowiedzialny za wdrożenie (deployment) modelu. Wysyła cały katalog z wytrenowanym predyktorem do usługi Azure Blob Storage.

Potok domyślny (`__default__`)
-----------------------------

Projekt definiuje również potok `__default__`, który jest uruchamiany po wywołaniu komendy `kedro run` bez dodatkowych argumentów. Potok ten stanowi sekwencyjne połączenie trzech powyższych potoków:

`data_processing` → `autogluon_model` → `azure_upload`

Taka struktura zapewnia pełną automatyzację procesu od surowych danych do wdrożonego modelu.

Szczegółowy opis potoków
------------------------

Poniżej znajdują się odnośniki do szczegółowej dokumentacji każdego z potoków, zawierającej opis jego struktury oraz poszczególnych węzłów (funkcji).

.. toctree::
   :maxdepth: 1

   pipelines/data_processing
   pipelines/autogluon_model
   pipelines/azure_upload