import numpy as np
from sklearn.datasets import make_regression
from sklearn.linear_model import Lasso, Ridge
from sklearn.preprocessing import StandardScaler
X, y, coef = make_regression(n_samples=306, n_features=8000, n_informative=50,
                    noise=0.1, shuffle=True, coef=True, random_state=42)

alpha = 0.1
scaler = StandardScaler()
scaler.fit(X)
X = scaler.transform(X)
ridge_reg = Ridge(alpha=alpha)
ridge_reg.fit(X, y)
weights = 1.0 / np.abs(ridge_reg.coef_)

# X_w = X / weights
# lasso_reg = Lasso(alpha=alpha)
# lasso_reg.fit(X_w, y)
# print(p_obj(lasso_reg.coef_/weights))


g = lambda w: np.abs(w)
gprime = lambda w: 1. / np.abs(w)

# g = lambda w: np.sqrt(np.abs(w))
# gprime = lambda w: 1. / (2. * np.sqrt(np.abs(w)) + np.finfo(float).eps)

# Or another option:
# ll = 0.01
# g = lambda w: np.log(ll + np.abs(w))
# gprime = lambda w: 1. / (ll + np.abs(w))

n_samples, n_features = X.shape
p_obj = lambda w: 1. / (2 * n_samples) * np.sum((y - np.dot(X, w)) ** 2) \
                  + alpha * np.sum(g(w))

n_lasso_iterations = 1

for k in range(n_lasso_iterations):
    X_w = X / weights[np.newaxis, :]
    clf = Lasso(alpha=alpha, fit_intercept=True)
    clf.fit(X_w, y)
    coef_ = clf.coef_ / weights
    weights = gprime(coef_)
    print(p_obj(coef_))


