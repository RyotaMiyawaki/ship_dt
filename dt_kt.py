#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from ship_mmg import kt_maneuver as kt

def estimate_kt_derivatives_by_l2method(vdr_df):
    u"""
    最小二乗法を用いてKとTを算出する
    :param vdr_df: 推定対象部分のみ切り出されたvdrデータ(r_delta, r, RudderAngle_radが入力されていることが前提)
    :return: k,t : 推定されたKとT
    """
    dt = vdr_df["Time"][1] - vdr_df["Time"][0] # [s]
    A = []
    B = []
    for num in range(len(vdr_df)-1):
        A.append([vdr_df["r_rad"][num], vdr_df["RudderAngle_rad"][num]])
        B.append([vdr_df["r_rad"][num+1]])
    A = np.array(A)
    B = np.array(B)
    THETA = np.linalg.pinv(A).dot(B)

    K = THETA[1][0] / (1.0 - THETA[0][0])
    T = dt / (1.0 - THETA[0][0])
    return K, T


def draw_parameter_graph_for_multiple_results(time_list, parameter_list, label_list, save_file_name):
    u"""
    シミュレーション結果の時系列データをpyplotで出力する.
    draw_parameter_graphと違って単位変換を含んでないので予め変換してからこのメソッドに突っ込むこと
    :param time_list : maneuverメソッドで出力される操縦性シミュレーションの結果に対応する時間リスト
    :param parameter_list : maneuverメソッドで出力される操縦性シミュレーションの結果のパラメータに関する情報のリスト
    :param label_list : parameter_listに対応してグラフの凡例につける名前のリスト(parameter_listと長さは同じ)
    :param save_file_name : 軌跡図の保存path
    """
    for num in range(len(parameter_list)):
        plt.plot(time_list, parameter_list[num], label=label_list[num])
    plt.legend()
    plt.xlabel('Time[s]')
    plt.savefig(save_file_name)
    plt.clf()

def draw_multiple_trajectory(p_list, p_label_list, save_file_name):
    u"""
    複数の軌跡図をpyplotで出力する
    :param p_list : 複数の操縦性シミュレーションの結果のxy座標情報のリスト情報
    :param p_label_list : p_listに対応してグラフの凡例につける名前のリスト(p_listと長さは同じ)
    :param save_file_name : 軌跡図の保存path
    """
    for num in range(len(p_list)):
        plt.plot(p_list[num][0], p_list[num][1], label=p_label_list[num])
    plt.legend()
    plt.xlabel('X[m]')
    plt.ylabel('Y[m]')
    plt.savefig(save_file_name)
    plt.clf()


if __name__ == '__main__':

    ## 1. Realデータ読み込みと前処理 (今回は、ship_mmgの可視化ビューからの出力csvを想定)
    csv = pd.read_csv(sys.argv[1])
    csv["r_rad"] = csv["r_degree"] * np.pi / 180
    # csv["r_delta"] = (csv["r"].diff(periods=1) / csv["Time"].diff(periods=1)).fillna(method='bfill').interpolate()
    csv["RudderAngle_rad"] = csv["RudderAngle_degree"] * np.pi / 180

    # 2. システム同定
    K,T = estimate_kt_derivatives_by_l2method(csv)
    print(K,T)

    # 3. システム同定結果からVirtualデータの作成
    x0 = csv["X"].values[0]
    y0 = csv["Y"].values[0]
    psi0 = csv["HDG_degree"].values[0] * np.pi / 180
    u_list = csv["u"].tolist()
    r0 = csv["r_rad"].values[0]
    duration = csv["Time"].tail(1).values[0]
    sampling = 1000
    delta_list = csv["RudderAngle_rad"].tolist()
    # print(K,T,x0,y0,psi0,u_list,r0,duration,sampling,delta_list)
    time, X = kt.maneuver(K,T,x0,y0,psi0,u_list,r0,duration,sampling,delta_list)

    # 4. 結果の比較
    # 4-1. 航跡
    p_virtual = [X.T[1].tolist(), X.T[2].tolist()]
    p_real = [csv["X"].tolist(), csv["Y"].tolist()]
    draw_multiple_trajectory([p_real, p_virtual], ['real', 'virtual'], 'trajectory.png')

    # 4-2. 各種値の時系列変化 (rad -> degreeに変更しているところもある)
    value_list = [
        # csv["RudderAngle"],
        # csv["HDG_degree"],
        # csv["u"],
        csv["r_degree"],
        # X.T[0] * 180 / np.pi,
        # X.T[3] * 180 / np.pi,
        # X.T[4],
        X.T[5] * 180 / np.pi
    ]
    label_list = [
        # "delta(real)",
        # "psi(real) [degree]",
        # "u(real)",
        "r(real) [degree/s]",
        # "delta(virtual)",
        # "psi(virtual) [degree]",
        # "u(virtual)",
        "r(virtual) [degree/s]"
    ]
    draw_parameter_graph_for_multiple_results(time, value_list, label_list, 'parameter.png')
