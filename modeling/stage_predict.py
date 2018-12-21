from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import numpy as np


def stage_score_plot(estimator, X_train, y_train, X_test, y_test, ax):
    '''
    Parameters: estimator: GradientBoostingRegressor or AdaBoostRegressor
                X_train: 2d numpy array
                y_train: 1d numpy array
                X_test: 2d numpy array
                y_test: 1d numpy array

    Returns: A plot of the number of iterations vs the MSE for the model for
    both the training set and test set.
    '''
    # fit estimator
    # estimator.fit(X_train, y_train)
    train_mse_at_stages = []
    test_mse_at_stages = []
    
    # iterate through all stages for test and train and record mean_square_errors lists
    for y1, y2 in zip(estimator.staged_predict(X_train), estimator.staged_predict(X_test)):
        train_mse = mean_squared_error(y_train, y1)
        train_mse_at_stages.append(train_mse)
        
        test_mse = mean_squared_error(y_test, y2)
        test_mse_at_stages.append(test_mse)

    # find the # of trees at which test error is the lowest
    lowest_test_error = np.argmin(test_mse_at_stages)

    # create xs in order to plot. each x represents n_estimators.
    xs = range(0, len(test_mse_at_stages))

        
    ax.plot(xs, train_mse_at_stages, label="{} Train".format(estimator.__class__.__name__))
    ax.plot(xs, test_mse_at_stages, label="{} Test".format(estimator.__class__.__name__))
    ax.axvline(lowest_test_error)
    print(lowest_test_error)


    # example of how to use:
    # fig, ax = plt.subplots(figsize=(12, 8))
    # gdbr_model.fit(X_train, y_train)
    # stage_score_plot(gdbr_model, X_train, y_train, X_test, y_test)
    # stage_score_plot(gdbr_model_2, X_train, y_train, X_test, y_test)
    # ax.legend()
    # plt.show()