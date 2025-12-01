"""
Project: Customer Churn, Segmentation & Lifetime Value
Focus: Feature extraction in SQL, clustering, churn model, and LTV
Source: Advanced portfolio spec by Rodrigo Blasi Olandoski
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.ensemble import RandomForestClassifier


DB_URL = "postgresql+psycopg2://user:password@localhost:5432/analytics_db"
engine = create_engine(DB_URL, future=True)


def load_customer_features() -> pd.DataFrame:
    query = "SELECT * FROM analytics.customer_features;"
    return pd.read_sql(query, engine)


def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    df["revenue_12m"] = df["revenue_12m"].fillna(0)
    df["avg_ticket_12m"] = df["avg_ticket_12m"].fillna(0)
    df["txn_count_12m"] = df["txn_count_12m"].fillna(0)
    df["revenue_12m_log"] = np.log1p(df["revenue_12m"])
    return df


def build_segments(df: pd.DataFrame, n_clusters: int = 4) -> pd.DataFrame:
    features_seg = df[["revenue_12m", "txn_count_12m", "avg_ticket_12m"]].copy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features_seg)
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df["segment_cluster"] = kmeans.fit_predict(X_scaled)
    return df


def train_churn_model(df: pd.DataFrame):
    feature_cols = [
        "revenue_12m",
        "txn_count_12m",
        "avg_ticket_12m",
        "distinct_categories_12m",
        "revenue_last_90d",
    ]
    X = df[feature_cols].copy()
    y = df["churn_label"].astype(int)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )
    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        random_state=42,
        class_weight="balanced",
    )
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    print(classification_report(y_test, y_pred))
    print("ROC AUC:", roc_auc_score(y_test, y_proba))
    df["churn_proba"] = model.predict_proba(X)[:, 1]
    return model, df


def compute_ltv(df: pd.DataFrame) -> pd.DataFrame:
    def _ltv(row):
        base = row["revenue_12m"]
        p = row["churn_proba"]
        if p < 0.3:
            mult = 3
        elif p < 0.6:
            mult = 2
        else:
            mult = 1
        return base * mult

    df["ltv_score"] = df.apply(_ltv, axis=1)
    return df


def export_scores(df: pd.DataFrame):
    df_export = df[[
        "customer_id",
        "segment_cluster",
        "churn_label",
        "churn_proba",
        "ltv_score",
    ]].copy()
    df_export.to_sql(
        "customer_scores",
        con=engine,
        schema="analytics",
        if_exists="replace",
        index=False,
    )
    print("Customer scores exported to analytics.customer_scores.")


if __name__ == "__main__":
    customers_df = load_customer_features()
    customers_df = prepare_data(customers_df)
    customers_df = build_segments(customers_df, n_clusters=4)
    model, customers_df = train_churn_model(customers_df)
    customers_df = compute_ltv(customers_df)
    export_scores(customers_df)
    print("Churn, segmentation & LTV pipeline completed.")
