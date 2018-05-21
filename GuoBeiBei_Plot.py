import datetime as dt
import os

import matplotlib.pyplot as plt
import pandas as pd

data_dir = 'data/GuoBeiBei'
fig_dir = 'image/GuoBeiBei'

fig1_dir = 'fig1'
fig2a_dir = 'fig2a'
fig2b_dir = 'fig2b'

if not os.path.exists(fig_dir):
    os.makedirs(fig_dir)


class Fig1(object):
    def __init__(self):
        self.data_dir = os.path.join(data_dir, '{}.csv'.format(fig1_dir))
        self.fig_dir = os.path.join(fig_dir, '{}.jpg'.format(fig1_dir))

    def plot(self):
        fig1_df = pd.read_csv(self.data_dir, index_col=0)
        fig1_df['Time'] = fig1_df['Time'].astype(dt.datetime)

        fig = plt.figure(figsize=(16, 9))

        ax1 = fig.add_subplot(211)
        line, = ax1.plot_date(fig1_df['Time'], fig1_df['Log Returns'], ls='-', c='k', marker=None)
        ax1.set_title('Log returns of Hong Kong Hang Seng (1988-01~2015-03)')

        ax2 = fig.add_subplot(212)
        line2, = ax2.plot_date(fig1_df['Time'], fig1_df['Monthly Index'], ls='-', c='k', marker=None)
        ax2.set_title('Monthly index of Hong Kong Hang Seng (1988-01~2015-03)')

        x_list = []
        y_list = []
        i_list = []
        for i, row in fig1_df.iterrows():
            if row['l2'] == 'Yes':
                ax2.axvline(row['Time'], ls='--', c='k')
                x_list.append(row['Time'])
                y_list.append(row['Monthly Index'])
                i_list.append(i)
            if row['l1'] == 'Yes':
                ax2.axvline(row['Time'], ls=':', c='k')
                x_list.append(row['Time'])
                y_list.append(row['Monthly Index'])
                i_list.append(i)

        xy_df = pd.DataFrame({
            'x': x_list,
            'y': y_list,
            'i': i_list
            })
        xy_df.drop_duplicates(inplace=True)
        xy_df.sort_values(by='i', inplace=True)

        ax2.annotate(xy_df.iloc[0]['x'][:-2],
                     xy=(fig1_df.iloc[xy_df.iloc[0]['i'] - 15]['Time'], xy_df.iloc[0]['y'] + 4000))
        ax2.annotate(xy_df.iloc[1]['x'][:-2],
                     xy=(fig1_df.iloc[xy_df.iloc[1]['i'] - 15]['Time'], xy_df.iloc[1]['y'] + 5000))
        ax2.annotate(xy_df.iloc[2]['x'][:-2], xy=(xy_df.iloc[2]['x'], xy_df.iloc[2]['y'] + 6000))
        ax2.annotate(xy_df.iloc[3]['x'][:-2], xy=(xy_df.iloc[3]['x'], xy_df.iloc[3]['y'] + 7000))
        ax2.annotate(xy_df.iloc[4]['x'][:-2], xy=(xy_df.iloc[4]['x'], xy_df.iloc[4]['y'] + 5000))
        ax2.annotate(xy_df.iloc[5]['x'][:-2],
                     xy=(fig1_df.iloc[xy_df.iloc[5]['i'] - 15]['Time'], xy_df.iloc[5]['y'] + 5000))
        ax2.annotate(xy_df.iloc[6]['x'][:-2], xy=(xy_df.iloc[6]['x'], xy_df.iloc[6]['y'] + 6000))
        ax2.annotate(xy_df.iloc[7]['x'][:-2], xy=(xy_df.iloc[7]['x'], xy_df.iloc[7]['y'] + 5000))
        ax2.annotate(xy_df.iloc[8]['x'][:-2],
                     xy=(fig1_df.iloc[xy_df.iloc[8]['i'] - 19]['Time'], xy_df.iloc[8]['y'] - 5000))
        ax2.annotate(xy_df.iloc[9]['x'][:-2], xy=(xy_df.iloc[9]['x'], xy_df.iloc[9]['y'] + 5000))

        for text in ax2.texts:
            text.set_bbox(dict(facecolor='white', edgecolor='None', alpha=0.5))

        fig.savefig(self.fig_dir)
        # plt.show()
        plt.close('all')


class Fig2(object):
    def __init__(self):
        self.data_dir = os.path.join(data_dir, '{}.csv'.format(fig2a_dir))
        self.data2_dir = os.path.join(data_dir, '{}.csv'.format(fig2b_dir))
        self.fig_dir = os.path.join(fig_dir, '{}.jpg'.format(fig2a_dir))
        self.fig2_dir = os.path.join(fig_dir, '{}.jpg'.format(fig2b_dir))

    def plot(self):

        def plot2(data, fig, loc, col):
            ax1 = fig.add_subplot(loc)
            line, = ax1.plot_date(data['Time'], data['a'], ls='-', c='k', ms=1.5)
            line_min, = ax1.plot_date(data['Time'], data['b'], ls='-.', c='k', marker=None)
            line_max, = ax1.plot_date(data['Time'], data['c'], ls='--', c='k', marker=None)
            ax1.set_title('1988-01~2015-03')

            for i, row in data.iterrows():
                if row[col] == 'Yes':
                    ax1.axvline(row['Time'], ls='--', c='k')
                    ax1.annotate(row['Time'][:-2], xy=(row['Time'], row['c'] - 1))
            for text in ax1.texts:
                text.set_bbox(dict(facecolor='white', edgecolor='None', alpha=0.5))

        fig2a_df = pd.read_csv(self.data_dir, index_col=0)
        fig2a_df['Time'] = fig2a_df['Time'].astype(dt.datetime)
        fig2b_df = pd.read_csv(self.data2_dir, index_col=0)
        fig2b_df['Time'] = fig2b_df['Time'].astype(dt.datetime)

        fig = plt.figure(figsize=(16, 9))
        fig2 = plt.figure(figsize=(16, 9))
        plot2(fig2a_df, fig, 111, 'l1')
        plot2(fig2b_df, fig2, 111, 'l')

        fig.savefig(self.fig_dir)
        fig2.savefig(self.fig2_dir)
        # plt.show()
        plt.close('all')


if __name__ == '__main__':
    Fig2().plot()
    Fig1().plot()
