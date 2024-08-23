import matplotlib.pyplot as plt
import statsmodels.api as sm
import pandas as pd
import numpy as np
import networkx as nx
import os

def boolMapping(b):
    if b == True:
        return 1
    if b == False:
        return 0
    if b == None:
        return -1

def hist2d(df, col1, col2, save=False, format='svg', show=True, bins=100, title=None):
    plt.hist2d(df[col1], df[col2], bins=bins, cmap='hot')
    plt.colorbar()
    plt.xlabel(col1)
    plt.ylabel(col2)
    title = ' '.join(['hist2d', col1, col2]) if title == None else title
    plt.title(title)
    title = title.replace(' ', '_')
    if type(save) == str:
        plt.savefig(os.path.join(save, '{}.{}'.format(title, format)))
    if show:
        plt.show()
    plt.close()

def scatter(df, col1, col2, save=False, format='svg', show=True, title=None):
    plt.scatter(df[col1], df[col2])
    plt.xlabel(col1)
    plt.ylabel(col2)
    title = ' '.join(['scatter', col1, col2]) if title == None else title
    plt.title(title)
    title = title.replace(' ', '_')
    if type(save) == str:
        plt.savefig(os.path.join(save, '{}.{}'.format(title, format)))
    if show:
        plt.show()
    plt.close()

def batch_scatter(df, col1, cols2, save=False, format='svg', show=True, title_list=None):
    for i, col2 in enumerate(cols2):
        scatter(df, col1, col2, save=save, format=format, show=show, title=title_list[i])

def CDF(df, col, save=False, format='svg', show=True, title=None):
    ecdf = sm.distributions.ECDF(df[col])
    x_values = np.sort(df[col].unique())
    y_values = ecdf(x_values)
    plt.step(x_values, y_values)
    plt.xticks(rotation=45)
    plt.xlabel(col)
    plt.ylabel('Fraction')
    title = ' '.join(['CDF', col]) if title == None else title
    plt.title(title)
    title = title.replace(' ', '_')
    if type(save) == str:
        plt.savefig(os.path.join(save, '{}.{}'.format(title, format)))
    if show:
        plt.show()
    plt.close()

def newCDF(df, col, save=False, format='svg', show=True, bins=100, title=None):
    count, bins_count = np.histogram(df[col], bins=bins)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)
    plt.plot(bins_count[1:], cdf)
    plt.xticks(rotation=45)
    plt.xlabel(col)
    plt.ylabel('Fraction')
    title = ' '.join(['CDF', col]) if title == None else title
    plt.title(title)
    title = title.replace(' ', '_')
    if type(save) == str:
        plt.savefig(os.path.join(save, '{}.{}'.format(title, format)))
    if show:
        plt.show()
    plt.close()

def adjmatrix(node_df, edge_df, graph_name, cmap=None, node_id='ch_id', sort_by=None, ascending=True, weight='weight', save=False, format='svg', show=True, title=None):
    if type(sort_by) == str:
        nodes = node_df.sort_values(sort_by, ascending=ascending)[node_id].to_list()
    else:
        nodes = None
    graph = nx.from_pandas_edgelist(edge_df, 'ch_id', 'forwarded_from_id', edge_attr=weight, create_using=nx.DiGraph())
    adj_matrix = nx.adjacency_matrix(graph, weight=weight, nodelist=nodes).toarray()

    plt.imshow(adj_matrix, interpolation='none', origin='upper', cmap=cmap)
    plt.colorbar()
    title = ' '.join(['adjmatrix', graph_name]) if title == None else title
    plt.title(title)
    title = title.replace(' ', '_')
    if type(save) == str:
        plt.savefig(os.path.join(save, '{}.{}'.format(title, format)))
    if show:
        plt.show()
    plt.close()

def get_df_from_TGgraph(node_df, edge_df, multiedge_df, id_name='id'):

    tmp1_df = edge_df.groupby('source')['count'].sum().reset_index().rename(columns={'source':id_name, 'count': 'n_forewards_by_me'})
    tmp2_df = node_df[node_df['out_degree']==0]['ch_ID'].to_frame()
    tmp2_df['n_forewards_by_me'] = 0
    node_df = node_df.merge(pd.concat([tmp1_df, tmp2_df]), on=id_name)

    tmp1_df = edge_df.groupby('target')['count'].sum().reset_index().rename(columns={'target':id_name, 'count': 'n_forewards_by_others'})
    tmp2_df = node_df[node_df['in_degree']==0]['ch_ID'].to_frame()
    tmp2_df['n_forewards_by_others'] = 0
    node_df = node_df.merge(pd.concat([tmp1_df, tmp2_df]), on=id_name)

    node_df['n_original_msgs'] = node_df['n_messages'] - node_df['n_forewards_by_me']
    node_df['original_forwarded_ratio'] = node_df['n_original_msgs'] / node_df['n_forewards_by_me']

    #node_df['new_creation_date'] = pd.to_datetime(node_df['creation_date'], unit='s')
    #multiedge_df['new_forwarded_message_date'] = pd.to_datetime(multiedge_df['forwarded_message_date'], unit='s')
    #multiedge_df['new_date'] = pd.to_datetime(multiedge_df['date'], unit='s')
    multiedge_df['delta_time'] = multiedge_df['date']-multiedge_df['forwarded_message_date']

    tmp1_df = multiedge_df.groupby('source')['delta_time'].mean().reset_index().rename(columns={'source':id_name, 'delta_time': 'source_delta_time'})
    tmp2_df = node_df[node_df['out_degree']==0]['ch_ID'].to_frame()
    tmp2_df['source_delta_time'] = 0
    node_df = node_df.merge(pd.concat([tmp1_df, tmp2_df]), on=id_name)

    tmp1_df = multiedge_df.groupby('target')['delta_time'].mean().reset_index().rename(columns={'target':id_name, 'delta_time': 'target_delta_time'})
    tmp2_df = node_df[node_df['in_degree']==0]['ch_ID'].to_frame()
    tmp2_df['target_delta_time'] = 0
    node_df = node_df.merge(pd.concat([tmp1_df, tmp2_df]), on=id_name)
    
    return node_df, edge_df, multiedge_df

def groupby_count_percentage(df, col):
    tmp = df[col].to_frame()
    tmp['count'] = 0
    tmp = tmp.groupby(col).count().reset_index().sort_values('count', ascending=False).reset_index(drop=True)
    tmp['percentage'] = (tmp['count']*100)/df.shape[0]
    return tmp