# Approximate line parameters per voltage class (GA data lacks impedance).
# 110/220 kV use pandapower's built-in std_type values (verified against
# pp.available_std_types(net, "line") on pandapower 3.4.0) since those are the
# only NEM voltage classes with a native match in pandapower's overhead-line
# catalog. No equivalent validated source exists for 66/132/275/330/500 kV, so
# those remain hand-set approximations within the standard textbook range.
_LINE_PARAMS = {
    500: {
        "r_ohm_per_km": 0.02,
        "x_ohm_per_km": 0.28,
        "c_nf_per_km": 12.0,
        "max_i_ka": 3.0,
    },
    330: {
        "r_ohm_per_km": 0.03,
        "x_ohm_per_km": 0.32,
        "c_nf_per_km": 11.0,
        "max_i_ka": 2.0,
    },
    275: {
        "r_ohm_per_km": 0.04,
        "x_ohm_per_km": 0.33,
        "c_nf_per_km": 11.0,
        "max_i_ka": 1.5,
    },
    220: {  # pandapower std_type "490-AL1/64-ST1A 220.0"
        "r_ohm_per_km": 0.059,
        "x_ohm_per_km": 0.285,
        "c_nf_per_km": 10.0,
        "max_i_ka": 0.96,
    },
    132: {
        "r_ohm_per_km": 0.10,
        "x_ohm_per_km": 0.40,
        "c_nf_per_km": 9.0,
        "max_i_ka": 0.6,
    },
    110: {  # pandapower std_type "184-AL1/30-ST1A 110.0"
        "r_ohm_per_km": 0.157,
        "x_ohm_per_km": 0.40,
        "c_nf_per_km": 8.80,
        "max_i_ka": 0.535,
    },
    66: {
        "r_ohm_per_km": 0.18,
        "x_ohm_per_km": 0.44,
        "c_nf_per_km": 8.0,
        "max_i_ka": 0.4,
    },
}
_DEFAULT_LINE_PARAMS = {
    "r_ohm_per_km": 0.10,
    "x_ohm_per_km": 0.40,
    "c_nf_per_km": 9.0,
    "max_i_ka": 0.6,
}
