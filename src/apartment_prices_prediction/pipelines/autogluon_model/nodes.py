import logging
from pathlib import Path
from typing import Tuple, List, Optional

import pandas as pd
from autogluon.tabular import TabularPredictor
from sklearn.model_selection import train_test_split

logger = logging.getLogger(__name__)


def split_data(
        df: pd.DataFrame, target_col: str, test_size: float = 0.2, random_state: int = 42
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Dzieli dane na zbiór treningowy i testowy.

    Args:
        df: Ramka danych do podziału.
        target_col: Nazwa kolumny docelowej.
        test_size: Procent danych, który ma stanowić zbiór testowy.
        random_state: Ziarno losowości dla powtarzalności podziału.

    Returns:
        Krotka zawierająca zbiór treningowy i testowy.
    """
    X = df.drop(columns=[target_col])
    y = df[target_col]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    train_data = pd.concat([X_train, y_train], axis=1)
    test_data = pd.concat([X_test, y_test], axis=1)
    logger.info(
        f"Podzielono dane. Zbiór treningowy: {len(train_data)} wierszy, zbiór testowy: {len(test_data)} wierszy.")
    return train_data, test_data


def train_autogluon_model(
        train_data: pd.DataFrame,
        label: str,
        model_path: str,
        excluded_features: Optional[List[str]] = None,
        time_limit: int = 600,
) -> str:
    """Trenuje model predykcyjny przy użyciu AutoGluon.

    Args:
        train_data: Ramka danych treningowych.
        label: Nazwa kolumny docelowej (etykiety).
        model_path: Ścieżka do zapisu wytrenowanego modelu.
        excluded_features: Lista kolumn do wykluczenia z treningu.
        time_limit: Limit czasu na trening w sekundach.

    Returns:
        Ścieżka do katalogu z zapisanym modelem.
    """
    if excluded_features:
        train_data = train_data.drop(columns=excluded_features, errors="ignore")

    model_dir = Path(model_path)
    model_dir.mkdir(parents=True, exist_ok=True)

    predictor = TabularPredictor(label=label, eval_metric="root_mean_squared_error", path=str(model_dir))
    predictor.fit(train_data, time_limit=time_limit)
    logger.info(f"Zakończono trening modelu AutoGluon. Model zapisany w: {model_path}")

    return model_path


def predict_with_autogluon(predictor_path: str, test_data: pd.DataFrame) -> pd.DataFrame:
    """Generuje predykcje na danych testowych przy użyciu wytrenowanego modelu AutoGluon.

    Args:
        predictor_path: Ścieżka do zapisanego modelu (predyktora).
        test_data: Ramka danych testowych.

    Returns:
        Ramka danych testowych z dodaną kolumną predykcji `predicted_price`.
    """
    logger.info(f"Ładowanie predyktora z: {predictor_path}")
    loaded_predictor = TabularPredictor.load(predictor_path)

    features = test_data.drop(columns=[loaded_predictor.label], errors="ignore")
    predictions = loaded_predictor.predict(features)

    test_data_with_predictions = test_data.copy()
    test_data_with_predictions["predicted_price"] = predictions
    logger.info("Pomyślnie wygenerowano predykcje.")
    return test_data_with_predictions
