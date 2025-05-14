import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def plot_drawdown(eq_df, outpath):

    plt.figure()
    eq_df['drawdown'].plot(title="Drawdown")
    plt.xlabel("Time")
    plt.ylabel("Drawdown")
    plt.tight_layout()
    plt.savefig(os.path.join(outpath, "drawdown.png"))
    plt.close()

    return


def plot_equity_curve(eq_df, outpath):
    
    plt.figure()
    eq_df['equity'].plot(title="Equity Curve")
    plt.xlabel("Time")
    plt.ylabel("Equity")
    plt.tight_layout()
    plt.savefig(os.path.join(outpath, "equity_curve.png"))
    plt.close()

    return


def export_summary(result, outpath, config):

    eq_df = pd.DataFrame(result['equity_curve'], columns=['t', 'equity'])
    eq_df['return'] = (eq_df['equity'].pct_change()).replace(float('nan'), 0)
    eq_df['log_return'] = np.log(1 + (eq_df['equity'].pct_change()).replace(float('nan'), 0))
    eq_df['cum_return'] = (eq_df['equity'] / config['starting_capital'])
    eq_df['excess_return'] = eq_df['return'] - (config['rfr'] / (365 * 24 * 60))
    eq_df['drawdown'] = (eq_df['equity'] / eq_df['equity'].cummax()) - 1

    sharpe = (eq_df['excess_return'].mean() / eq_df['excess_return'].std()) * np.sqrt((365 * 24 * 60))
    max_dd = eq_df['drawdown'].min()
    cum_return = eq_df['cum_return'].iloc[-1]
    num_days = (eq_df['t'].iloc[-1] - eq_df['t'].iloc[0]) / (60 * 60 * 24 * 100)
    annual_return = (eq_df['cum_return'].iloc[-1] ** (365 / num_days)) - 1

    summary_txt = os.path.join(outpath, 'summary.txt')
    with open(summary_txt, 'w') as f:
        f.write(f"Annualized Return: {annual_return:.2%}\n")
        f.write(f"Sharpe Ratio: {sharpe:.2f}\n")
        f.write(f"Max Drawdown: {max_dd:.2%}\n")

    plot_drawdown(eq_df, outpath)
    plot_equity_curve(eq_df, outpath)
    eq_df.to_csv(os.path.join(outpath, 'data.csv'), index=False)

    return


