#!/bin/env python3
import os
import sys
import pandas as pd
from datetime import datetime
import math
import time

data_folder = 'datasets'
rebalancing_ratio = 0.04  # Le taux de rebalancement
default_weight = 0.03
start_date = pd.to_datetime('2023-01-01 00:00:00')
end_date = pd.to_datetime('2023-11-04 01:20:00')
limit = 100000
data = {}
portfolio = {}
portfolio_weights = {}
symbols = []
symbolsPlusAda = ['ADA']
proportion_cible = {}
max_absolu = {}
capital_initial = {}
rebalancing_count = 0
outputfile = "dataset_fusion.csv"

#################
# DATASETS init #
#################
def initialize_datasets():
    global data
    global symbols
    global symbolsPlusAda
    for filename in os.listdir(data_folder):
        # removing buggy datasets (floats) and non-interesting ones
        if "wrt" not in filename and "ieth" not in filename and "milk" not in filename and "cneta" not in filename and "snek" not in filename :
            if filename.endswith('.csv'):
                symbol = filename.split('-')[1].split('.')[0]
                df = pd.read_csv(os.path.join(data_folder, filename), header=None)
                df.drop(df.columns[2], axis=1, inplace=True)
                #[df.groupby(df.iloc[:, 0]).df.iloc[:, 1].idxmin()]
                df.loc[df.groupby(0)[1].idxmin()]
                df = df.drop_duplicates(subset=0)
                df[df.columns[1]] = df[df.columns[1]].rolling(window=9).mean() # Moyenne mobile sur 9 entrées
                df[df.columns[0]] = pd.to_datetime(df.iloc[:, 0])
                filtered_data = df[(df.iloc[:, 0] >= start_date) & (df.iloc[:, 0] <= end_date)]
                if not filtered_data.empty:
                    data[symbol] = filtered_data
                    if symbol not in symbols:
                        symbols.append(symbol)
                        symbolsPlusAda.append(symbol)
    combined_df = pd.DataFrame({'Date': pd.date_range(start_date, end_date, freq='S')})
    combined_df['Date'] = pd.to_datetime(combined_df['Date'])
    for symbol, df in data.items():
        df = df.rename(columns={0: 'Date', 1: symbol})
        df['Date'] = pd.to_datetime(df['Date'])  # Convertir la colonne 'Date' en datetime64
        df = df.drop_duplicates(subset='Date')
        combined_df = combined_df.merge(df, on='Date', how='left')
    combined_df = combined_df.fillna(method='ffill')
    combined_df = combined_df.fillna(method='bfill')
    combined_df = combined_df.drop_duplicates(subset=combined_df.columns.difference(['Date']))
    data = combined_df
    print(data)
    data.to_csv(outputfile)

##################
# PORTFOLIO INIT #
##################
def initialize_portfolio():
    global portfolio
    global proportion_cible
    capital_initial = {symbol: 0 for symbol in symbols}
    ### definir le poids cible de chaque CNT, en ratio, ADA prends le reste.
    proportion_cible = {symbol: default_weight for symbol in symbols} # poids max d'un CNT
    #proportion_cible['ibtc'] = 0.13 # ratio custom pour ibtc
    total_relatif = sum(proportion_cible[symbol] for symbol in symbols)
    proportion_cible['ADA'] = round(1 - total_relatif, 3) # ADA prends le reste
    if proportion_cible['ADA'] <= 0:
        raise ValueError("La valeur de max_relatif['ADA'] est invalide. Controlez les poids, le nombre d'assets, etc...")
    #max_absolu = {symbol: 1000 for symbol in symbols}
    #max_absolu['ADA'] = 1000000
    capital_initial['ADA'] = 10000 # capital au début
    #capital_initial['ibtc'] = 0.1
    portfolio = capital_initial
    return portfolio

##############################################################################
######################## PORTFOLIO ITERATION LOOP ############################
##############################################################################

def iteration_portfolio(line):
    global portfolio_weights
    global portfolio
    row = data.iloc[line]
    totalInAda = balanceInAda(row)
    #port_prop(row)
    rebalance(row)

def balanceInAda(row):
    global totalInAda
    totalInAda = 0
    for symbol in symbols:
        totalInAda += round(row[symbol] * portfolio[symbol], 6)
    totalInAda += round(portfolio['ADA'],6)
    return round(totalInAda,6)

def balanceInIusd(row):
    total = balanceInAda(row) / row['iusd']
    return total

def balanceInIbtc(row):
    total = balanceInAda(row) / row['ibtc']
    return total

def portfolio_proportions(row):
    global portfolio_weights
    totalInAda = balanceInAda(row)
    ada_weight = portfolio['ADA'] / totalInAda
    portfolio_weights['ADA'] = round(ada_weight,4)
    for symbol in symbols:
        asset_value = portfolio[symbol] * row[symbol]
        weight = asset_value / totalInAda
        portfolio_weights[symbol] = round(weight,4)
    return portfolio_weights

def rebalance(row):
    global txcount
    global portfolio
    global portfolio_weights
    global rebalancing_count
    portfolio_weights = portfolio_proportions(row)
    totalInAda = balanceInAda(row)
    rebalance_needed = 0
    tx_details = ""
    test = {}
    for symbol in symbolsPlusAda:
        if symbol == 'ADA':
            current_value = portfolio_weights[symbol]
            target_value = proportion_cible[symbol]
        else :
            current_value = portfolio_weights[symbol]
            target_value = proportion_cible[symbol] 
        if abs(current_value - target_value) > rebalancing_ratio:
            rebalance_needed = 1
            tx_details += ">>> " + str(symbol) + " à déclenché un rééquilibrage. poids : " + str(portfolio_weights[symbol]) + " != " + str(target_value) + " poids idéal\n"
            break

    if rebalance_needed == 1:
        rebalancing_count += 1
        print("---- REBALANCING --------- "+ str(row['Date']) + "----------------")
        #print(str(row))
        print("portfolio avant tx: " + str(portfolio))
        print("portfolio_weights : " + str(portfolio_weights))
        print("portfolio valeur en ADA avant swap : " + str(totalInAda))
        for symbol in symbolsPlusAda:
            if symbol == 'ADA':
                portfolio[symbol] = round(totalInAda * proportion_cible[symbol], 6)
                tx_details += str(portfolio[symbol]) + " " + symbol + "\n"
            else :
                portfolio[symbol] = round(totalInAda * proportion_cible[symbol] / row[symbol],6)
                tx_details += str(portfolio[symbol]) + " " + symbol + " au prix : " + str(row[symbol]) + "\n"
        print(tx_details)
        portfolio_proportions(row)
        ################## VÉRIF
        check = 1
        details = ""
        for symbol in symbolsPlusAda :
            check -= round(portfolio_weights[symbol],4)
            details += symbol + " " + str(portfolio_weights[symbol]) + " " + str(check) + "\n"
        if round(check,4) != 0:
            print(details)
            raise "problème de poids : {details}"
        #################################
        print("total en ADA : " + str(totalInAda))
        print("portfolio après tx: " + str(portfolio))
        print("portfolio_weights : " + str(portfolio_weights))
#
#
# Début de la logique
#

if len(sys.argv) > 0:
    print("dataset loading : "+ sys.argv[1])
    combined_dataset = pd.read_csv(sys.argv[1])
    data = combined_dataset
else:
    combined_dataset = initialize_datasets()

initialize_portfolio()


end = len(data)-1
print("------------- : DÉBUT : ----------- " + str(start_date))
print("nombre de tokens : " + str(len(symbolsPlusAda)))
print("rebalancing_ratio : " + str(rebalancing_ratio))
print("proportions cible : " + str(proportion_cible))
print("portfolio du départ : " + str(portfolio))
#print("portfolio_weights : " + str(portfolio_weights))

total_debut = balanceInAda(data.iloc[0])
print("total en ADA au début : " + str(total_debut))
total_debut_iusd = balanceInIusd(data.iloc[0])
total_debut_ibtc = balanceInIbtc(data.iloc[0])
print("total en iUSD au début : " + str(total_debut_iusd))
print("total en iBTC au début : " + str(total_debut_ibtc))

# main loop : 
for i in range(len(data)):
    iteration_portfolio(i)
    if rebalancing_count == limit:
        break


print("------------ : FIN  : " + str(end_date))
print(data.iloc[end])
total_fin = balanceInAda(data.iloc[end])
difference = total_fin - total_debut
pourcentage_difference = (difference / total_debut) * 100
total_fin_iusd = balanceInIusd(data.iloc[end])
difference = total_fin_iusd - total_debut_iusd
pourcentage_difference_iusd = (difference / total_debut_iusd) * 100
total_fin_ibtc = balanceInIbtc(data.iloc[end])
difference = total_fin_ibtc - total_debut_ibtc
pourcentage_difference_ibtc = (difference / total_debut_ibtc) * 100


print("total en iUSD au final : " + str(total_fin_iusd) + "   ( "+ str(pourcentage_difference_iusd) + " % )")
print("total en ADA au final : "+str(total_fin) + "   ( " + str(pourcentage_difference) + " % )")
print("total en iBTC au final : "+str(total_fin_ibtc) + "   ( " + str(pourcentage_difference_ibtc) + " % )")

print("portfolio d'arrivée : " + str(portfolio))
print("portfolio_weights : " + str(portfolio_weights))
print("nombre de rééquilibrage au total : "+str(rebalancing_count))

print("i :"+str(i))
