import numpy as np
# UTCI_approx, Version a 0.002, October 2009
# Copyright (C) 2009  Peter Broede


def run_utci_calculations(mrt_rast, row):
    """
    Compute Universal Thermal Climate Index (UTCI) from air temperature, wind speed, humidity, and MRT.
    https://utci.org/

    Parameters
    - mrt_rast: (raster array) Mean radiant temperature (°C)
    - row: (pandas.Series) Must include keys:"Temperature" (°C), "Wind Speed" (m/s), "Relative Humidity" (%)

    Returns
    - utci_values: (raster array) UTCI value(s) in °C
    """
    at = float(row["Temperature"])
    va = float(row["Wind Speed"])
    rh = float(row["Relative Humidity"])

    # Calculate vpd from rh and at
    g = np.array([
        -2836.5744, -6028.076559, 19.54263612, -0.02737830188,
        0.000016261698, 7.0229056e-10, -1.8680009e-13
    ], dtype=float)

    # air temp in K
    tk = at + 273.15
    es = 2.7150305 * np.log(tk)
    for i, gi in enumerate(g, start=1):
        es = es + (gi * (tk ** (i - 2)))

    # Convert from Pa to hPa
    es = np.exp(es) * 0.01
    # Actual vapor pressure in hPa
    vpd = es * (rh / 100.0)

    # Make sure all the input values are valid
    _validate_met_and_mrt(at, va, float(vpd), mrt_rast)

    # Calculate UTCI
    utci_values = _calculate_utci(at=at, ehpa=float(vpd), va=va, tmrt=mrt_rast)

    return utci_values


def run_utci_cat_calculations(utci_rast):
    """
    Categorize UTCI values into thermal stress categories.
    Categories:
    <= -40: Extreme Cold Stress (1)
    -40 to -27: Very Strong Cold Stress (2)
    -27 to -13: Strong Cold Stress (3)
    -13 to 0: Moderate Cold Stress (4)
    0 to 9: Slight Cold Stress (5)
    9 to 26: No Thermal Stress (6)
    26 to 32: Moderate Heat Stress (7)
    32 to 38: Strong Heat Stress (8)
    38 to 46: Very Strong Heat Stress (9)
    > 46: Extreme Heat Stress (10)

    Parameters
    - utci_rast: (raster array) UTCI value(s) in °C.

    Returns
    - utci_cat: (raster array) UTCI categories 
    """
    utci_cat = np.full(utci_rast.shape, np.nan)

    utci_cat[utci_rast < -40] = 1
    utci_cat[(utci_rast >= -40) & (utci_rast < -27)] = 2
    utci_cat[(utci_rast >= -27) & (utci_rast < -13)] = 3
    utci_cat[(utci_rast >= -13) & (utci_rast < 0)] = 4
    utci_cat[(utci_rast >= 0) & (utci_rast < 9)] = 5
    utci_cat[(utci_rast >= 9) & (utci_rast < 26)] = 6
    utci_cat[(utci_rast >= 26) & (utci_rast < 32)] = 7
    utci_cat[(utci_rast >= 32) & (utci_rast < 38)] = 8
    utci_cat[(utci_rast >= 38) & (utci_rast < 46)] = 9
    utci_cat[utci_rast >= 46] = 10

    return utci_cat


def _validate_met_and_mrt(at, wind, vpd, tmrt):
    if not (-50 <= at <= 50):
        raise ValueError("Temperature must be between -50 and 50 C!")
    if not (0.5 <= wind <= 17):
        raise ValueError("Wind must be between 0.5 and 17 m/s!")
    if not (0 <= vpd <= 50):
        raise ValueError(
            "Vapor pressure deficit must be between 0 and 50 hPa!")

    arr = np.asarray(tmrt)
    arr_min = np.nanmin(arr)
    arr_max = np.nanmax(arr)

    if (arr_min < (at - 50)) or (arr_max > (at + 70)):
        raise ValueError(
            "MRT must be between 50 C below and 70 C above the air temp!")


def _calculate_utci(at, ehpa, va, tmrt):
    """
    sources for this code include
    https://gist.github.com/chriswmackey/b852dd9e01f79402d8a7b355f97b84c8
    https://james-ramsden.com/calculate-utci-c-code/
    eOUBLE PRECISION Function value is the UTCI in degree Celsius
    computed by a 6th order approximating polynomial from the 4 Input paramters 
    Input parameters (all of type double precision)
    at : air temperature, degree Celsius
    ehpa : water vapour presure, hPa=hecto Pascal
    va : wind speed 10 m above ground level in m/s
    tmrt : mean radiant temperature, degree Celsius
    """
    # Convert Tmrt to ndarray (keeps scalar behavior too)
    tmrt_arr = np.asarray(tmrt, dtype=float)

    # Check if inputs are numeric and non-null
    if np.isnan(at) or np.isnan(ehpa) or np.isnan(va) or np.all(np.isnan(tmrt_arr)):
        raise ValueError("All input parameters must be non-NA values.")
    if any(x is None for x in (at, ehpa, va, tmrt)):
        raise ValueError("All input parameters must be non-null values.")

    # Calculate the difference between mean radiant temperature and air temperature
    d_tmrt = tmrt_arr - at
    # Convert vapor pressure from hPa to kPa
    pa = ehpa / 10.0

    # Apply the full UTCI approximation formula
    utci_approx = (
        at +
        (6.07562052e-01) +
        (-2.27712343e-02) * at +
        (8.06470249e-04) * at*at +
        (-1.54271372e-04) * at*at*at +
        (-3.24651735e-06) * at*at*at*at +
        (7.32602852e-08) * at*at*at*at*at +
        (1.35959073e-09) * at*at*at*at*at*at +
        (-2.25836520e+00) * va +
        (8.80326035e-02) * at*va +
        (2.16844454e-03) * at*at*va +
        (-1.53347087e-05) * at*at*at*va +
        (-5.72983704e-07) * at*at*at*at*va +
        (-2.55090145e-09) * at*at*at*at*at*va +
        (-7.51269505e-01) * va*va +
        (-4.08350271e-03) * at*va*va +
        (-5.21670675e-05) * at*at*va*va +
        (1.94544667e-06) * at*at*at*va*va +
        (1.14099531e-08) * at*at*at*at*va*va +
        (1.58137256e-01) * va*va*va +
        (-6.57263143e-05) * at*va*va*va +
        (2.22697524e-07) * at*at*va*va*va +
        (-4.16117031e-08) * at*at*at*va*va*va +
        (-1.27762753e-02) * va*va*va*va +
        (9.66891875e-06) * at*va*va*va*va +
        (2.52785852e-09) * at*at*va*va*va*va +
        (4.56306672e-04) * va*va*va*va*va +
        (-1.74202546e-07) * at*va*va*va*va*va +
        (-5.91491269e-06) * va*va*va*va*va*va +
        (3.98374029e-01) * d_tmrt +
        (1.83945314e-04) * at*d_tmrt +
        (-1.73754510e-04) * at*at*d_tmrt +
        (-7.60781159e-07) * at*at*at*d_tmrt +
        (3.77830287e-08) * at*at*at*at*d_tmrt +
        (5.43079673e-10) * at*at*at*at*at*d_tmrt +
        (-2.00518269e-02) * va*d_tmrt +
        (8.92859837e-04) * at*va*d_tmrt +
        (3.45433048e-06) * at*at*va*d_tmrt +
        (-3.77925774e-07) * at*at*at*va*d_tmrt +
        (-1.69699377e-09) * at*at*at*at*va*d_tmrt +
        (1.69992415e-04) * va*va*d_tmrt +
        (-4.99204314e-05) * at*va*va*d_tmrt +
        (2.47417178e-07) * at*at*va*va*d_tmrt +
        (1.07596466e-08) * at*at*at*va*va*d_tmrt +
        (8.49242932e-05) * va*va*va*d_tmrt +
        (1.35191328e-06) * at*va*va*va*d_tmrt +
        (-6.21531254e-09) * at*at*va*va*va*d_tmrt +
        (-4.99410301e-06) * va*va*va*va*d_tmrt +
        (-1.89489258e-08) * at*va*va*va*va*d_tmrt +
        (8.15300114e-08) * va*va*va*va*va*d_tmrt +
        (7.55043090e-04) * d_tmrt*d_tmrt +
        (-5.65095215e-05) * at*d_tmrt*d_tmrt +
        (-4.52166564e-07) * at*at*d_tmrt*d_tmrt +
        (2.46688878e-08) * at*at*at*d_tmrt*d_tmrt +
        (2.42674348e-10) * at*at*at*at*d_tmrt*d_tmrt +
        (1.54547250e-04) * va*d_tmrt*d_tmrt +
        (5.24110970e-06) * at*va*d_tmrt*d_tmrt +
        (-8.75874982e-08) * at*at*va*d_tmrt*d_tmrt +
        (-1.50743064e-09) * at*at*at*va*d_tmrt*d_tmrt +
        (-1.56236307e-05) * va*va*d_tmrt*d_tmrt +
        (-1.33895614e-07) * at*va*va*d_tmrt*d_tmrt +
        (2.49709824e-09) * at*at*va*va*d_tmrt*d_tmrt +
        (6.51711721e-07) * va*va*va*d_tmrt*d_tmrt +
        (1.94960053e-09) * at*va*va*va*d_tmrt*d_tmrt +
        (-1.00361113e-08) * va*va*va*va*d_tmrt*d_tmrt +
        (-1.21206673e-05) * d_tmrt*d_tmrt*d_tmrt +
        (-2.18203660e-07) * at*d_tmrt*d_tmrt*d_tmrt +
        (7.51269482e-09) * at*at*d_tmrt*d_tmrt*d_tmrt +
        (9.79063848e-11) * at*at*at*d_tmrt*d_tmrt*d_tmrt +
        (1.25006734e-06) * va*d_tmrt*d_tmrt*d_tmrt +
        (-1.81584736e-09) * at*va*d_tmrt*d_tmrt*d_tmrt +
        (-3.52197671e-10) * at*at*va*d_tmrt*d_tmrt*d_tmrt +
        (-3.36514630e-08) * va*va*d_tmrt*d_tmrt*d_tmrt +
        (1.35908359e-10) * at*va*va*d_tmrt*d_tmrt*d_tmrt +
        (4.17032620e-10) * va*va*va*d_tmrt*d_tmrt*d_tmrt +
        (-1.30369025e-09) * d_tmrt*d_tmrt*d_tmrt*d_tmrt +
        (4.13908461e-10) * at*d_tmrt*d_tmrt*d_tmrt*d_tmrt +
        (9.22652254e-12) * at*at*d_tmrt*d_tmrt*d_tmrt*d_tmrt +
        (-5.08220384e-09) * va*d_tmrt*d_tmrt*d_tmrt*d_tmrt +
        (-2.24730961e-11) * at*va*d_tmrt*d_tmrt*d_tmrt*d_tmrt +
        (1.17139133e-10) * va*va*d_tmrt*d_tmrt*d_tmrt*d_tmrt +
        (6.62154879e-10) * d_tmrt*d_tmrt*d_tmrt*d_tmrt*d_tmrt +
        (4.03863260e-13) * at*d_tmrt*d_tmrt*d_tmrt*d_tmrt*d_tmrt +
        (1.95087203e-12) * va*d_tmrt*d_tmrt*d_tmrt*d_tmrt*d_tmrt +
        (-4.73602469e-12) * d_tmrt*d_tmrt*d_tmrt*d_tmrt*d_tmrt*d_tmrt +
        (5.12733497e+00) * pa +
        (-3.12788561e-01) * at*pa +
        (-1.96701861e-02) * at*at*pa +
        (9.99690870e-04) * at*at*at*pa +
        (9.51738512e-06) * at*at*at*at*pa +
        (-4.66426341e-07) * at*at*at*at*at*pa +
        (5.48050612e-01) * va*pa +
        (-3.30552823e-03) * at*va*pa +
        (-1.64119440e-03) * at*at*va*pa +
        (-5.16670694e-06) * at*at*at*va*pa +
        (9.52692432e-07) * at*at*at*at*va*pa +
        (-4.29223622e-02) * va*va*pa +
        (5.00845667e-03) * at*va*va*pa +
        (1.00601257e-06) * at*at*va*va*pa +
        (-1.81748644e-06) * at*at*at*va*va*pa +
        (-1.25813502e-03) * va*va*va*pa +
        (-1.79330391e-04) * at*va*va*va*pa +
        (2.34994441e-06) * at*at*va*va*va*pa +
        (1.29735808e-04) * va*va*va*va*pa +
        (1.29064870e-06) * at*va*va*va*va*pa +
        (-2.28558686e-06) * va*va*va*va*va*pa +
        (-3.69476348e-02) * d_tmrt*pa +
        (1.62325322e-03) * at*d_tmrt*pa +
        (-3.14279680e-05) * at*at*d_tmrt*pa +
        (2.59835559e-06) * at*at*at*d_tmrt*pa +
        (-4.77136523e-08) * at*at*at*at*d_tmrt*pa +
        (8.64203390e-03) * va*d_tmrt*pa +
        (-6.87405181e-04) * at*va*d_tmrt*pa +
        (-9.13863872e-06) * at*at*va*d_tmrt*pa +
        (5.15916806e-07) * at*at*at*va*d_tmrt*pa +
        (-3.59217476e-05) * va*va*d_tmrt*pa +
        (3.28696511e-05) * at*va*va*d_tmrt*pa +
        (-7.10542454e-07) * at*at*va*va*d_tmrt*pa +
        (-1.24382300e-05) * va*va*va*d_tmrt*pa +
        (-7.38584400e-09) * at*va*va*va*d_tmrt*pa +
        (2.20609296e-07) * va*va*va*va*d_tmrt*pa +
        (-7.32469180e-04) * d_tmrt*d_tmrt*pa +
        (-1.87381964e-05) * at*d_tmrt*d_tmrt*pa +
        (4.80925239e-06) * at*at*d_tmrt*d_tmrt*pa +
        (-8.75492040e-08) * at*at*at*d_tmrt*d_tmrt*pa +
        (2.77862930e-05) * va*d_tmrt*d_tmrt*pa +
        (-5.06004592e-06) * at*va*d_tmrt*d_tmrt*pa +
        (1.14325367e-07) * at*at*va*d_tmrt*d_tmrt*pa +
        (2.53016723e-06) * va*va*d_tmrt*d_tmrt*pa +
        (-1.72857035e-08) * at*va*va*d_tmrt*d_tmrt*pa +
        (-3.95079398e-08) * va*va*va*d_tmrt*d_tmrt*pa +
        (-3.59413173e-07) * d_tmrt*d_tmrt*d_tmrt*pa +
        (7.04388046e-07) * at*d_tmrt*d_tmrt*d_tmrt*pa +
        (-1.89309167e-08) * at*at*d_tmrt*d_tmrt*d_tmrt*pa +
        (-4.79768731e-07) * va*d_tmrt*d_tmrt*d_tmrt*pa +
        (7.96079978e-09) * at*va*d_tmrt*d_tmrt*d_tmrt*pa +
        (1.62897058e-09) * va*va*d_tmrt*d_tmrt*d_tmrt*pa +
        (3.94367674e-08) * d_tmrt*d_tmrt*d_tmrt*d_tmrt*pa +
        (-1.18566247e-09) * at*d_tmrt*d_tmrt*d_tmrt*d_tmrt*pa +
        (3.34678041e-10) * va*d_tmrt*d_tmrt*d_tmrt*d_tmrt*pa +
        (-1.15606447e-10) * d_tmrt*d_tmrt*d_tmrt*d_tmrt*d_tmrt*pa +
        (-2.80626406e+00) * pa*pa +
        (5.48712484e-01) * at*pa*pa +
        (-3.99428410e-03) * at*at*pa*pa +
        (-9.54009191e-04) * at*at*at*pa*pa +
        (1.93090978e-05) * at*at*at*at*pa*pa +
        (-3.08806365e-01) * va*pa*pa +
        (1.16952364e-02) * at*va*pa*pa +
        (4.95271903e-04) * at*at*va*pa*pa +
        (-1.90710882e-05) * at*at*at*va*pa*pa +
        (2.10787756e-03) * va*va*pa*pa +
        (-6.98445738e-04) * at*va*va*pa*pa +
        (2.30109073e-05) * at*at*va*va*pa*pa +
        (4.17856590e-04) * va*va*va*pa*pa +
        (-1.27043871e-05) * at*va*va*va*pa*pa +
        (-3.04620472e-06) * va*va*va*va*pa*pa +
        (5.14507424e-02) * d_tmrt*pa*pa +
        (-4.32510997e-03) * at*d_tmrt*pa*pa +
        (8.99281156e-05) * at*at*d_tmrt*pa*pa +
        (-7.14663943e-07) * at*at*at*d_tmrt*pa*pa +
        (-2.66016305e-04) * va*d_tmrt*pa*pa +
        (2.63789586e-04) * at*va*d_tmrt*pa*pa +
        (-7.01199003e-06) * at*at*va*d_tmrt*pa*pa +
        (-1.06823306e-04) * va*va*d_tmrt*pa*pa +
        (3.61341136e-06) * at*va*va*d_tmrt*pa*pa +
        (2.29748967e-07) * va*va*va*d_tmrt*pa*pa +
        (3.04788893e-04) * d_tmrt*d_tmrt*pa*pa +
        (-6.42070836e-05) * at*d_tmrt*d_tmrt*pa*pa +
        (1.16257971e-06) * at*at*d_tmrt*d_tmrt*pa*pa +
        (7.68023384e-06) * va*d_tmrt*d_tmrt*pa*pa +
        (-5.47446896e-07) * at*va*d_tmrt*d_tmrt*pa*pa +
        (-3.59937910e-08) * va*va*d_tmrt*d_tmrt*pa*pa +
        (-4.36497725e-06) * d_tmrt*d_tmrt*d_tmrt*pa*pa +
        (1.68737969e-07) * at*d_tmrt*d_tmrt*d_tmrt*pa*pa +
        (2.67489271e-08) * va*d_tmrt*d_tmrt*d_tmrt*pa*pa +
        (3.23926897e-09) * d_tmrt*d_tmrt*d_tmrt*d_tmrt*pa*pa +
        (-3.53874123e-02) * pa*pa*pa +
        (-2.21201190e-01) * at*pa*pa*pa +
        (1.55126038e-02) * at*at*pa*pa*pa +
        (-2.63917279e-04) * at*at*at*pa*pa*pa +
        (4.53433455e-02) * va*pa*pa*pa +
        (-4.32943862e-03) * at*va*pa*pa*pa +
        (1.45389826e-04) * at*at*va*pa*pa*pa +
        (2.17508610e-04) * va*va*pa*pa*pa +
        (-6.66724702e-05) * at*va*va*pa*pa*pa +
        (3.33217140e-05) * va*va*va*pa*pa*pa +
        (-2.26921615e-03) * d_tmrt*pa*pa*pa +
        (3.80261982e-04) * at*d_tmrt*pa*pa*pa +
        (-5.45314314e-09) * at*at*d_tmrt*pa*pa*pa +
        (-7.96355448e-04) * va*d_tmrt*pa*pa*pa +
        (2.53458034e-05) * at*va*d_tmrt*pa*pa*pa +
        (-6.31223658e-06) * va*va*d_tmrt*pa*pa*pa +
        (3.02122035e-04) * d_tmrt*d_tmrt*pa*pa*pa +
        (-4.77403547e-06) * at*d_tmrt*d_tmrt*pa*pa*pa +
        (1.73825715e-06) * va*d_tmrt*d_tmrt*pa*pa*pa +
        (-4.09087898e-07) * d_tmrt*d_tmrt*d_tmrt*pa*pa*pa +
        (6.14155345e-01) * pa*pa*pa*pa +
        (-6.16755931e-02) * at*pa*pa*pa*pa +
        (1.33374846e-03) * at*at*pa*pa*pa*pa +
        (3.55375387e-03) * va*pa*pa*pa*pa +
        (-5.13027851e-04) * at*va*pa*pa*pa*pa +
        (1.02449757e-04) * va*va*pa*pa*pa*pa +
        (-1.48526421e-03) * d_tmrt*pa*pa*pa*pa +
        (-4.11469183e-05) * at*d_tmrt*pa*pa*pa*pa +
        (-6.80434415e-06) * va*d_tmrt*pa*pa*pa*pa +
        (-9.77675906e-06) * d_tmrt*d_tmrt*pa*pa*pa*pa +
        (8.82773108e-02) * pa*pa*pa*pa*pa +
        (-3.01859306e-03) * at*pa*pa*pa*pa*pa +
        (1.04452989e-03) * va*pa*pa*pa*pa*pa +
        (2.47090539e-04) * d_tmrt*pa*pa*pa*pa*pa +
        (1.48348065e-03) * pa*pa*pa*pa*pa*pa
    )

    return utci_approx
