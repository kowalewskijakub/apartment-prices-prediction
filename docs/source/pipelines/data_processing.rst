.. _pipeline_data_processing:

Potok przetwarzania danych (data_processing)
============================================

Cel
---
Potok `data_processing` jest pierwszym i fundamentalnym etapem całego procesu. Jego zadaniem jest przekształcenie surowych, podzielonych na partycje danych w jeden, spójny i oczyszczony zbiór danych, który będzie stanowił wejście dla modelu uczenia maszynowego.

Struktura potoku
----------------
Potok składa się z następujących, sekwencyjnie wykonywanych węzłów:

1.  **`concatenate_monthly_data_node`** – łączy dane z wielu partycji (plików CSV) w jedną ramkę danych,
2.  **`impute_numerical_columns_node`** – uzupełnia brakujące wartości w kolumnach numerycznych,
3.  **`impute_categorical_columns_node`** – uzupełnia brakujące wartości w kolumnach kategorycznych,
4.  **`outlier_removal_node`** – identyfikuje i usuwa wartości odstające z kluczowych kolumn numerycznych,
5.  **`feature_engineering_node`** – tworzy nowe, syntetyczne cechy (np. wiek budynku),
6.  **`normalization_node`** – normalizuje wartości w wybranych kolumnach numerycznych.

Opis węzłów (Nodes)
-------------------

Poniżej znajduje się szczegółowy opis funkcji implementujących poszczególne węzły potoku, wygenerowany automatycznie na podstawie docstringów w kodzie.

.. autofunction:: apartment_prices_prediction.pipelines.data_processing.nodes.concatenate_data

.. autofunction:: apartment_prices_prediction.pipelines.data_processing.nodes.impute_numerical_columns

.. autofunction:: apartment_prices_prediction.pipelines.data_processing.nodes.impute_categorical_columns

.. autofunction:: apartment_prices_prediction.pipelines.data_processing.nodes.remove_outliers

.. autofunction:: apartment_prices_prediction.pipelines.data_processing.nodes.feature_engineering

.. autofunction:: apartment_prices_prediction.pipelines.data_processing.nodes.normalize_numerical_columns