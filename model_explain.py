import shap

def explain(iso, X_scaled, X_raw, features, label):
    explainer  = shap.TreeExplainer(iso)
    sv_raw     = explainer.shap_values(X_scaled)
    sv         = sv_raw[0] if isinstance(sv_raw, list) else sv_raw

    print(f'── {label}: global feature importance ──')
    shap.summary_plot(sv, X_raw, feature_names=features, plot_type='bar', show=True)

    top_pos  = X_raw.reset_index(drop=True).index[
        pd.Series(iso.decision_function(X_scaled)).idxmin()
    ]
    base_val = explainer.expected_value
    if hasattr(base_val, '__len__'): base_val = float(base_val[0])

    shap.plots.waterfall(
        shap.Explanation(
            values        = sv[top_pos],
            base_values   = base_val,
            data          = X_scaled[top_pos],
            feature_names = features,
        )
    )

explain(iso_hosp, X_hosp_scaled, X_hosp_raw, HOSPITAL_FEATURES, 'Hospital')
explain(iso_ltc,  X_ltc_scaled,  X_ltc_raw,  LTC_FEATURES,      'LTC')