import pandas as pd
from dagster import (
    AssetCheckResult,
    Definitions,
    MetadataValue,
    Output,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
)
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

import db

ACCURACY_THRESHOLD = 0.6
FEATURE_INPUT_COLUMNS = ["quantity", "price", "category"]


def build_order_features(orders: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    merged = orders.merge(products, on="product_id", how="inner")
    merged["total"] = merged["quantity"] * merged["price"]
    median_total = merged["total"].median()
    merged["is_high_value"] = (merged["total"] > median_total).astype(int)
    return merged[
        [
            "order_id",
            "product_id",
            "customer_id",
            "quantity",
            "price",
            "category",
            "total",
            "is_high_value",
        ]
    ]


def feature_metadata(features: pd.DataFrame) -> dict:
    """Plain-dict summary of a features DataFrame, used to populate
    Dagster's Output(metadata=...) for `order_features`. Kept as a pure
    function (no Dagster imports) so it's trivial to unit test."""
    return {
        "num_rows": len(features),
        "columns": list(features.columns),
        "high_value_rate": float(features["is_high_value"].mean()) if len(features) else 0.0,
    }


def train_classifier(features: pd.DataFrame) -> dict:
    encoded = pd.get_dummies(features[FEATURE_INPUT_COLUMNS], columns=["category"])
    feature_columns = list(encoded.columns)
    labels = features["is_high_value"]

    x_train, x_test, y_train, y_test = train_test_split(
        encoded, labels, test_size=0.3, random_state=42
    )
    model = LogisticRegression(max_iter=1000)
    model.fit(x_train, y_train)
    accuracy = accuracy_score(y_test, model.predict(x_test))

    return {
        "model": model,
        "accuracy": accuracy,
        "feature_columns": feature_columns,
        "num_train_rows": len(x_train),
        "num_test_rows": len(x_test),
    }


def training_metadata(model_bundle: dict) -> dict:
    """Plain-dict summary of a trained model bundle, used to populate
    Dagster's Output(metadata=...) for `trained_model`."""
    return {
        "accuracy": model_bundle["accuracy"],
        "feature_names": model_bundle["feature_columns"],
        "num_train_rows": model_bundle["num_train_rows"],
        "num_test_rows": model_bundle["num_test_rows"],
    }


def score_orders(features: pd.DataFrame, model_bundle: dict) -> pd.DataFrame:
    encoded = pd.get_dummies(features[FEATURE_INPUT_COLUMNS], columns=["category"])
    encoded = encoded.reindex(columns=model_bundle["feature_columns"], fill_value=0)
    model = model_bundle["model"]
    predicted = model.predict(encoded)
    probability = model.predict_proba(encoded)[:, 1]
    return pd.DataFrame(
        {
            "order_id": features["order_id"].values,
            "predicted_label": predicted,
            "probability": probability,
            "actual_label": features["is_high_value"].values,
        }
    )


@asset
def order_features() -> Output[pd.DataFrame]:
    orders = db.read_table("orders")
    products = db.read_table("products")
    features = build_order_features(orders, products)
    meta = feature_metadata(features)
    return Output(
        value=features,
        metadata={
            "num_rows": MetadataValue.int(meta["num_rows"]),
            "columns": MetadataValue.json(meta["columns"]),
            "high_value_rate": MetadataValue.float(meta["high_value_rate"]),
            "preview": MetadataValue.text(features.head().to_string(index=False)),
        },
    )


@asset
def trained_model(order_features: pd.DataFrame) -> Output[dict]:
    model_bundle = train_classifier(order_features)
    meta = training_metadata(model_bundle)
    return Output(
        value=model_bundle,
        metadata={
            "accuracy": MetadataValue.float(meta["accuracy"]),
            "feature_names": MetadataValue.json(meta["feature_names"]),
            "num_train_rows": MetadataValue.int(meta["num_train_rows"]),
            "num_test_rows": MetadataValue.int(meta["num_test_rows"]),
        },
    )


@asset_check(asset=trained_model)
def model_quality_check(trained_model: dict) -> AssetCheckResult:
    accuracy = trained_model["accuracy"]
    return AssetCheckResult(
        passed=accuracy >= ACCURACY_THRESHOLD, metadata={"accuracy": accuracy}
    )


@asset
def order_value_predictions(order_features: pd.DataFrame, trained_model: dict) -> Output[int]:
    predictions = score_orders(order_features, trained_model)
    row_count = db.load_table(predictions, "order_value_predictions")
    return Output(
        value=row_count,
        metadata={
            "num_rows": MetadataValue.int(row_count),
            "table_name": MetadataValue.text("order_value_predictions"),
        },
    )


refresh_ml_job = define_asset_job(name="refresh_ml_job")

refresh_ml_weekly = ScheduleDefinition(
    name="refresh_ml_weekly",
    job=refresh_ml_job,
    cron_schedule="0 6 * * 1",
)

defs = Definitions(
    assets=[order_features, trained_model, order_value_predictions],
    asset_checks=[model_quality_check],
    jobs=[refresh_ml_job],
    schedules=[refresh_ml_weekly],
)
