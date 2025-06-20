.. _pipeline_autogluon_model:

Potok trenowania modelu (autogluon_model)
=========================================

Cel
---
Potok `autogluon_model` odpowiada za cały cykl życia modelu uczenia maszynowego. Jego celem jest wytrenowanie, na podstawie przygotowanych danych, wydajnego modelu predykcyjnego przy użyciu biblioteki AutoML - AutoGluon.

Struktura potoku
----------------
Potok składa się z trzech kluczowych węzłów:

1.  **`split_data_node`** – dzieli przetworzony zbiór danych na część treningową i testową, co jest standardową praktyką w uczeniu maszynowym,
2.  **`train_autogluon_model_node`** – używa zbioru treningowego do wytrenowania predyktora AutoGluon, proces ten obejmuje automatyczny dobór modeli, hiperparametrów oraz tworzenie zespołów modeli (ensembling),
3.  **`predict_with_autogluon_node`** – wczytuje wytrenowany model i wykorzystuje go do wygenerowania predykcji cen na zbiorze testowym.

Opis węzłów (Nodes)
-------------------

Poniżej znajduje się szczegółowy opis funkcji implementujących poszczególne węzły potoku.

.. autofunction:: apartment_prices_prediction.pipelines.autogluon_model.nodes.split_data

.. autofunction:: apartment_prices_prediction.pipelines.autogluon_model.nodes.train_autogluon_model

.. autofunction:: apartment_prices_prediction.pipelines.autogluon_model.nodes.predict_with_autogluon