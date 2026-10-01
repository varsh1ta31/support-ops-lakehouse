"""Train and compare two SLA-breach classifiers from a versioned Delta dataset."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from support_ops.training_dataset import FEATURE_COLUMNS
from support_ops.transformations.gold import validate_hour
from support_ops.transformations.silver import qualified

NUMERIC_FEATURES = tuple(
    name for name in FEATURE_COLUMNS if name not in ("priority", "support_tier", "customer_segment")
)
CATEGORICAL_FEATURES = ("priority", "support_tier", "customer_segment")
MAX_DRIVER_ROWS = 100_000


def validate_cohorts(rows: Sequence[Any]) -> dict[str, dict[str, int]]:
    """Refuse empty, single-class, or duplicate-ticket chronological cohorts."""
    counts = {split: {"rows": 0, "breaches": 0} for split in ("train", "validation", "test")}
    seen: set[str] = set()
    for row in rows:
        split = row["split"]
        label = row["label"]
        ticket_id = row["ticket_id"]
        if split not in counts or label not in (0, 1) or ticket_id in seen:
            raise ValueError("training dataset has an invalid split, label, or duplicate ticket")
        seen.add(ticket_id)
        counts[split]["rows"] += 1
        counts[split]["breaches"] += label
    for split, values in counts.items():
        if not 0 < values["breaches"] < values["rows"]:
            raise ValueError(f"{split} must contain both breach and non-breach tickets")
    return counts


def choose_candidate(results: dict[str, dict[str, dict[str, float]]]) -> str:
    """Select using validation only; never use test metrics for model choice."""
    return max(
        results,
        key=lambda name: (
            results[name]["validation"]["pr_auc"],
            results[name]["validation"]["recall"],
            results[name]["validation"]["precision"],
            -list(results).index(name),
        ),
    )


def acceptance_failures(metrics: dict[str, float], *, breach_rate: float) -> list[str]:
    """Gate deployment after validation selection using untouched test performance."""
    failures = []
    if metrics["pr_auc"] < 1.2 * breach_rate:
        failures.append("test PR-AUC below 1.2 times breach prevalence")
    if metrics["roc_auc"] < 0.6:
        failures.append("test ROC-AUC below 0.60")
    if metrics["precision"] <= breach_rate:
        failures.append("test precision does not beat breach prevalence")
    if metrics["recall"] < 0.3:
        failures.append("test recall below 0.30")
    return failures


def choose_threshold(y_true: Any, probability: Any) -> float:
    """Maximize validation F1; prefer higher precision and threshold on ties."""
    from sklearn.metrics import precision_recall_curve

    precision, recall, thresholds = precision_recall_curve(y_true, probability)
    return float(
        thresholds[
            max(
                range(len(thresholds)),
                key=lambda index: (
                    2 * precision[index] * recall[index] / (precision[index] + recall[index])
                    if precision[index] + recall[index]
                    else 0,
                    precision[index],
                    thresholds[index],
                ),
            )
        ]
    )


def evaluate(y_true: Any, probability: Any, *, threshold: float) -> dict[str, float]:
    from sklearn.metrics import (
        average_precision_score,
        f1_score,
        precision_score,
        recall_score,
        roc_auc_score,
    )

    prediction = probability >= threshold
    return {
        "precision": float(precision_score(y_true, prediction, zero_division=0)),
        "recall": float(recall_score(y_true, prediction, zero_division=0)),
        "f1": float(f1_score(y_true, prediction, zero_division=0)),
        "pr_auc": float(average_precision_score(y_true, probability)),
        "roc_auc": float(roc_auc_score(y_true, probability)),
    }


def candidate_pipeline(name: str) -> Any:
    from sklearn.compose import ColumnTransformer
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import LogisticRegression
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import OneHotEncoder, StandardScaler

    numeric = Pipeline(
        [
            ("impute", SimpleImputer(strategy="median", keep_empty_features=True)),
            ("scale", StandardScaler()),
        ]
    )
    categorical = Pipeline(
        [
            ("impute", SimpleImputer(strategy="constant", fill_value="unknown")),
            ("encode", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )
    prep = ColumnTransformer(
        [
            ("numeric", numeric, list(NUMERIC_FEATURES)),
            ("categorical", categorical, list(CATEGORICAL_FEATURES)),
        ],
        sparse_threshold=0,
    )
    if name == "logistic_regression":
        model = LogisticRegression(max_iter=1000, random_state=42)
    elif name == "gradient_boosted_tree":
        model = HistGradientBoostingClassifier(
            max_iter=100, max_depth=4, learning_rate=0.05, random_state=42
        )
    else:
        raise ValueError(f"unknown candidate: {name}")
    return Pipeline([("preprocess", prep), ("model", model)])


def train_models(
    spark: Any, *, catalog: str, label_as_of: datetime, pipeline_run_id: str
) -> dict[str, Any]:
    """Log both fitted artifacts and report a validation-selected candidate."""
    import mlflow
    import mlflow.sklearn
    import pandas as pd
    from mlflow.tracking import MlflowClient
    from pyspark.sql import functions as f

    label_as_of = validate_hour(label_as_of)
    if not pipeline_run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    table = qualified(catalog, "ml", "training_dataset")
    version = int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])
    frame = (
        spark.read.option("versionAsOf", version)
        .table(table)
        .filter(f.col("label_as_of") == f.lit(label_as_of))
    )
    row_count = frame.count()
    if row_count > MAX_DRIVER_ROWS:
        raise ValueError(f"training dataset exceeds {MAX_DRIVER_ROWS} driver rows")
    rows = frame.select("ticket_id", "as_of", "split", "label", *FEATURE_COLUMNS).collect()
    cohorts = validate_cohorts(rows)
    data = pd.DataFrame([row.asDict() for row in rows])
    for column in NUMERIC_FEATURES:
        data[column] = pd.to_numeric(data[column], errors="coerce").astype(float)
    data[list(CATEGORICAL_FEATURES)] = data[list(CATEGORICAL_FEATURES)].fillna("unknown")
    periods = {
        split: {
            "first_as_of": data.loc[data["split"] == split, "as_of"].min().isoformat(),
            "last_as_of": data.loc[data["split"] == split, "as_of"].max().isoformat(),
            **cohorts[split],
        }
        for split in cohorts
    }
    owner = spark.sql("SELECT current_user() AS name").first()["name"]
    experiment = f"/Users/{owner}/support-ops-{catalog}-sla-risk"
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(experiment)
    results: dict[str, dict[str, dict[str, float]]] = {}
    artifacts: dict[str, dict[str, str]] = {}
    for name in ("logistic_regression", "gradient_boosted_tree"):
        model = candidate_pipeline(name)
        training = data.loc[data["split"] == "train"]
        model.fit(training[list(FEATURE_COLUMNS)], training["label"])
        validation = data.loc[data["split"] == "validation"]
        validation_probability = model.predict_proba(validation[list(FEATURE_COLUMNS)])[:, 1]
        threshold = choose_threshold(validation["label"], validation_probability)
        with mlflow.start_run(run_name=f"{name}-{pipeline_run_id}") as run:
            mlflow.log_params(
                {
                    "model_type": name,
                    "dataset_table": f"{catalog}.ml.training_dataset",
                    "dataset_version": version,
                    "label_as_of": label_as_of.isoformat(),
                    "pipeline_run_id": pipeline_run_id,
                    "threshold": threshold,
                    "threshold_policy": "maximize_validation_f1",
                    "random_state": 42,
                }
            )
            mlflow.log_params(
                {
                    f"hyper_{key}": str(value)
                    for key, value in model.named_steps["model"].get_params(deep=False).items()
                }
            )
            mlflow.log_dict(
                {
                    "feature_set": list(FEATURE_COLUMNS),
                    "training_periods": periods,
                    "model_parameters": model.named_steps["model"].get_params(),
                },
                "training_manifest.json",
            )
            results[name] = {}
            for split in ("validation", "test"):
                cohort = data.loc[data["split"] == split]
                probability = (
                    validation_probability
                    if split == "validation"
                    else model.predict_proba(cohort[list(FEATURE_COLUMNS)])[:, 1]
                )
                metrics = evaluate(cohort["label"], probability, threshold=threshold)
                results[name][split] = metrics
                for metric, value in metrics.items():
                    mlflow.log_metric(f"{split}_{metric}", value)
            mlflow.log_metric("train_rows", cohorts["train"]["rows"])
            info = mlflow.sklearn.log_model(
                model, artifact_path="model", input_example=training[list(FEATURE_COLUMNS)].head(3)
            )
            artifacts[name] = {
                "run_id": run.info.run_id,
                "model_uri": info.model_uri,
                "threshold": str(threshold),
            }
    selected = choose_candidate(results)
    test_rate = cohorts["test"]["breaches"] / cohorts["test"]["rows"]
    failures = acceptance_failures(results[selected]["test"], breach_rate=test_rate)
    decision = "rejected" if failures else "accepted_for_registration"
    client = MlflowClient()
    client.set_tag(artifacts[selected]["run_id"], "deployment_decision", decision)
    client.set_tag(artifacts[selected]["run_id"], "acceptance_failures", "; ".join(failures))
    summary = {
        "experiment": experiment,
        "dataset_version": version,
        "label_as_of": label_as_of.isoformat(),
        "cohorts": periods,
        "metrics": results,
        "artifacts": artifacts,
        "selected_by_validation": selected,
        "deployment_decision": decision,
        "acceptance_failures": failures,
    }
    print(json.dumps(summary, sort_keys=True))
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--label-as-of", required=True)
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    train_models(
        SparkSession.builder.getOrCreate(),
        catalog=args.catalog,
        label_as_of=datetime.fromisoformat(args.label_as_of),
        pipeline_run_id=args.pipeline_run_id,
    )
    return 0
