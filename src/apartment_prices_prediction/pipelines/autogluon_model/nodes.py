import pandas as pd
from autogluon.tabular import TabularPredictor
from sklearn.model_selection import train_test_split


def split_data(df: pd.DataFrame, target_col: str, test_size: float = 0.2, random_state: int = 42):
    X = df.drop(columns=[target_col])
    y = df[target_col]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    train_data = pd.concat([X_train, y_train], axis=1)
    test_data = pd.concat([X_test, y_test], axis=1)
    return train_data, test_data


def train_autogluon_model(train_data: pd.DataFrame, label: str, time_limit: int = 600):
    predictor = TabularPredictor(label=label, eval_metric="root_mean_squared_error")
    predictor.fit(train_data, time_limit=time_limit)
    return predictor


def predict_autogluon_model(predictor: TabularPredictor, test_data: pd.DataFrame):
    predictions = predictor.predict(test_data.drop(columns=[predictor.label]))
    test_data["predicted_price"] = predictions
    return test_data
