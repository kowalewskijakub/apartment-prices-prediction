.. _pipeline_azure_upload:

Potok wysyłania modelu do Azure (azure_upload)
==============================================

Cel
---
Potok `azure_upload` realizuje zadanie wdrożenia (deploymentu) wytrenowanego modelu. Jego jedynym celem jest wysłanie całego katalogu z modelem AutoGluon do chmurowej usługi przechowywania obiektów typu blob – Azure Blob Storage.

Struktura potoku
----------------
Potok ten jest bardzo prosty i składa się z jednego węzła:

1.  **`upload_model_to_azure_node`** – nawiązuje połączenie z kontem Azure Storage, a następnie wysyła wszystkie pliki z lokalnego katalogu modelu do dedykowanego kontenera.

Warunki wykonania
-----------------
Działanie tego potoku jest uzależnione od obecności zmiennej środowiskowej `AZURE_STORAGE_CONNECTION_STRING`. Jeśli zmienna nie jest zdefiniowana, węzeł zakończy działanie bez próby wysłania plików, logując odpowiedni komunikat.

Opis węzłów (Nodes)
-------------------

Poniżej znajduje się szczegółowy opis funkcji implementującej jedyny węzeł tego potoku.

.. autofunction:: apartment_prices_prediction.pipelines.azure_upload.nodes.upload_model_to_blob