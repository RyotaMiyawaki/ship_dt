#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import numpy as np
import pandas as pd
import pandas.io.sql as sql
import matplotlib.pyplot as plt
from scipy import signal

u"MTI共同研究用:sqlite3に格納された情報をCRUDするためのモジュール"

default_style = 'ggplot'

def __create_table(db_pass, table_name):
    u"""
    sqlite3のDBを指定(db_pass)して、新しくtable_nameという名前のテーブルを作成する.
    :param db_pass: sqlite3のDBのパス
    :param table_name: 新しく作成するtableの名前
    :return:
    """
    create_table_query = 'CREATE TABLE ' + table_name + '("" integer,Time text,Altitude integer,Geoidal integer,Lat real,Lon real,Course_over_ground real,Speed_over_ground real,status_water_speed real,HDG real,Rate_of_turn real,Rel_Wind_Dir real,Rel_Wind_Spd real,RudderAngle real,Override text,RudderOrder real,SteeringMode text,Depth real,"Engine Speed/revolutions" real,Engine number real,True_Wind_Dir real,True_Wind_Spd real)'
    conn = sqlite3.connect(db_pass)
    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()
    conn.close()


def __insert(db_pass, table_name, insert_value_list):
    u"""
    sqlite3のDBを指定(db_pass)して、queryを元にデータをinsertする.
    insert_value_listは('',Time,Altitude,...)の形のデータがlist形式で登録されていることを想定
    :param db_pass: sqlite3のDBのパス
    :param table_name: DB内のtableのname
    :param insert_value_list: 挿入するvalueのリスト(('',Time,Altitude,...)の形のデータがlist形式で登録されていることを想定)
    :return:
    """
    base_query = 'INSERT INTO ' + table_name + ' VALUES '
    conn = sqlite3.connect(db_pass)
    cur = conn.cursor()
    for value in insert_value_list:
        v = str(value).replace('u', '')  # あまりうまくない方法なので、エラーが出る場合は要検討
        query = base_query + v
        cur.execute(query)
    conn.commit()
    conn.close()


def __read(db_pass, query):
    u"""
    sqlite3のDBを指定(db_pass)して、queryを元に得られたデータを配列で返す
    :param db_pass: sqlite3のDBのパス
    :param query: sqlite3の検索クエリ
    :return: results : 検索結果
    """
    conn = sqlite3.connect(db_pass)
    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()
    return results


def __update(db_pass, table_name, query):
    u"""
    sqlite3のDBを指定(db_pass)して、query(SET以降)を元にデータをupdateする
    :param db_pass: sqlite3のDBパス
    :param table_name: DB内のtableのname
    :param query: update命令を含むquery
    :return:
    """
    base_query = 'UPDATE ' + table_name + ' ' + query
    conn = sqlite3.connect(db_pass)
    cur = conn.cursor()
    cur.execute(base_query)
    conn.commit()
    conn.close()


def __delete_table(db_pass, table_name):
    u"""
    sqlite3のDBを指定(db_pass)して、table_nameと一致するTableを削除する.
    :param db_pass: sqlite3のDBパス
    :param table_name: DB内で削除したいtableのname
    :return:
    """
    query = 'DROP TABLE ' + table_name
    conn = sqlite3.connect(db_pass)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    conn.close()


def make_query(table_name, column_name, start_time, end_time):
    u"""
    時間の範囲を指定したクエリ文を作成するためのメソッド.
    :param table_name: sqlite3DBのtableのname
    :param column_name: 抽出したいcolumnの名前(column_nameについては、連続して入力してください（例えば、Lat,Lon,Time,...という風に）)
    :param start_time: 抽出対象データの開始時間
    :param end_time: 抽出対象データの終了時間
    :return: query文
    """
    if column_name != '*':
        column_list = column_name.split(",")
        column_name = '"' + '","'.join(column_list) + '"'

    query = 'SELECT ' + column_name + ' FROM ' + table_name
    if start_time == '' and end_time == '':
        pass
    elif start_time == '':
        query += ' WHERE \'' + end_time + '\' >= Time'
    elif end_time == '':
        query += ' WHERE \'' + start_time + '\' <= Time'
    else:
        query += ' WHERE \'' + end_time + '\' >= Time and \'' + start_time + '\' <= Time'
    return query


def extract_data_as_pandas(db_pass, table_name, column_name, start_time, end_time):
    u"""
    sqlite3のDBから条件を指定してデータを抽出し、pandas形式で出力する.
    :param db_pass: sqlite3のDBパス
    :param table_name: DB内のtableのname
    :param column_name: 抽出対象のcolumn(column_nameについては、連続して入力してください（例えば、「Lat,Lon,Time」という風に）
    :param start_time: 抽出対象期間(開始)
    :param end_time: 抽出対象期間(終了)
    :return: pandas形式で抽出したデータ
    """
    query = make_query(table_name, column_name, start_time, end_time)
    print(query)
    print(db_pass)
    conn = sqlite3.connect(db_pass)
    data_pandas = sql.read_sql(query, conn)
    conn.close()

    if column_name.find('Time') >= 0 or column_name == '*':
        data_pandas['Time'] = pd.to_datetime(data_pandas['Time'])

    return data_pandas


def add_lowpass_filter_data(df, column_name):
    u"""
    column_nameに該当するcolumnデータにローパスフィルタを適用したデータをdfに追加する.
    :param df: DataFrame形式のデータ(Timeのcolumnがあることが前提)
    :param column_name: ローパスフィルタを適用するcolumn
    :return: ローパスフィルタを適用した結果のcolumnを追加したdf
    """
    df = df.dropna()  # nullが入っている行は自動的に削除する
    df['Time'] = pd.to_datetime(df.Time)
    df.set_index('Time', inplace=True)

    # gyroデータの設定
    dt = 0.1  # サンプリング間隔[s]
    fn = 1 / (2 * dt)  # ナイキスト周波数[Hz]

    # lowpass filter パラメータ設定a
    fp = 0.04  # 通過域端周波数[Hz]
    fs = 0.10  # 阻止域端周波数[Hz]
    gpass = 0.2  # 通過域最大損失量[dB]
    gstop = 30  # 阻止域最小減衰量[dB]

    # 正規化
    Wp = fp / fn
    Ws = fs / fn

    # バターワースフィルタ
    N, Wn = signal.buttord(Wp, Ws, gpass, gstop)
    b1, a1 = signal.butter(N, Wn, "low")
    df[column_name + '_lowpass(butter)'] = signal.filtfilt(b1, a1, np.array(df[column_name]))

    # # 第一種チェビシェフフィルタ
    # N, Wn = signal.cheb1ord(Wp, Ws, gpass, gstop)
    # b2, a2 = signal.cheby1(N, gpass, Wn, "low")
    # df[column_name + '_lowpass(cheby1)'] = signal.filtfilt(b2, a2, np.array(df[column_name]))

    # # 第二種チェビシェフフィルタ
    # N, Wn = signal.cheb2ord(Wp, Ws, gpass, gstop)
    # b3, a3 = signal.cheby2(N, gstop, Wn, "low")
    # df[column_name + '_lowpass(cheby2)'] = signal.filtfilt(b3, a3, np.array(df[column_name]))

    # # 楕円フィルタ
    # N, Wn = signal.ellipord(Wp, Ws, gpass, gstop)
    # b4, a4 = signal.ellip(N, gpass, gstop, Wn, "low")
    # df[column_name + '_lowpass(ellip)'] = signal.filtfilt(b4, a4, np.array(df[column_name]))

    # # ベッセルフィルタ
    # N = 4
    # b5, a5 = signal.bessel(N, Ws, "low")
    # df[column_name + '_lowpass(bessel)'] = signal.filtfilt(b5, a5, np.array(df[column_name]))

    return df


def plot_graph(df, save_graph_path, style=default_style):
    u"""
    DataFrame形式のデータから時系列グラフをプロットする.
    :param df: DataFrame型のデータ
    :param save_graph_path: プロット図の保存場所
    :param style: 描写のスタイル(plt : defaultは'ggplot')
    :return:
    """
    plt.style.use(style)  # chart設定
    df.plot()
    plt.savefig(save_graph_path)
    print('Save picture to "' + str(save_graph_path) + '"')


def plot_xy_graph(df, x, y, save_graph_path, style=default_style):
    u"""
    DataFrame形式のデータからxyグラフをプロットする.
    :param df: DataFrame型のデータ
    :param x: x
    :param y: y
    :param save_graph_path: プロット図の保存場所
    :param style: 描写のスタイル(plt : defaultは'ggplot')
    :return:
    """
    plt.style.use(style)  # chart設定
    df.plot(kind='scatter', x=x, y=y)
    plt.savefig(save_graph_path)
    print('Save picture to "' + str(save_graph_path) + '"')


# -------------------------------
# ここから応用
# -------------------------------

def make_kml_line(db_pass, table_name, start_time, end_time, output_file_path):
    u"""
    sqlite3のDBからtableを指定して、緯度経度情報をGoogle Earthで線として可視化するためのkmlファイルを出力します.
    注意) 現在、デフォルトで分単位で出力するようにしています.
    :param db_pass: sqlite3のDBパス
    :param table_name: DB内のtableのname
    :param start_time: 対象の時間(開始)
    :param end_time: 対象の時間(終了)
    :param output_file_path: 出力ファイルパス
    :return:
    """
    # ------------QUERY作成-------------
    query = make_query(table_name, 'Lat,Lon,Time', start_time, end_time)  # 1. 出力する時間の範囲
    query += ' and Time LIKE \'%%%%-%%-%% %%:%%:00\''  # 2. 単位時間(ex.分オーダーで取得など..)
    query += ' ORDER BY Time'  # 3. 出力する順番(時間順にソート)
    # ----------------------------------
    print(query)
    results = __read(db_pass, query)

    f = open(output_file_path, 'w')
    f.write(
        '<?xml version="1.0" encoding="UTF-8"?><kml xmlns="http://earth.google.com/kml/2.0"><Document><Placemark><LineString><tessellate>1</tessellate><altitudeMode>relativeToGround</altitudeMode><coordinates>\n')
    for lat, lon, time in results:
        if lat != '':  # 空データの対応(飛ばす)
            f.write(str(lon) + "," + str(lat) + ",10\n")
    f.write('</coordinates></LineString></Placemark></Document></kml>')
    f.close()


def plot_graph_from_db(db_pass, table_name, column_name, start_time, end_time, save_graph_path, style=default_style):
    u"""
    sqlite3のDBからtableのcolume_nameを指定して、範囲内の時系列データをグラフ上にプロットする。
    ただし、nullが入っている行は自動的に削除するので注意
    :param db_pass: sqlite3のDBパス
    :param table_name: DB内のtableのname
    :param start_time: 対象の時間(開始)
    :param end_time: 対象の時間(終了)
    :param save_graph_path: 出力ファイルパス
    :param style: 描写のスタイル(plt : defaultは'ggplot')
    :return:
    """

    # Timeがある前提のメソッドなので、なければ勝手にTimeをクエリに追加する
    if column_name.find('Time') == -1:
        column_name = 'Time,' + column_name

    df = extract_data_as_pandas(db_pass, table_name, column_name, start_time, end_time)
    df = df.dropna()  # nullが入っている行は自動的に削除する
    df['Time'] = pd.to_datetime(df.Time)
    df.set_index('Time', inplace=True)
    plot_graph(df, save_graph_path, style)


def plot_xy_graph_from_db(db_pass, table_name, x_column_name, y_column_name, start_time, end_time, save_graph_path, style=default_style):
    u"""
    sqlite3のDBからtableのcolumn_name(x,y)を指定して、範囲内のデータをxyグラフ上にプロットする。
    ただし、nullが入っている行は自動的に削除するので注意
    :param db_pass: sqlite3のDBパス
    :param table_name: DB内のtableのname
    :param x_column_name: x軸に採用するcolumnのname
    :param y_column_name: y軸に採用するcolumnのname
    :param start_time: 対象の時間(開始)
    :param end_time: 対象の時間(終了)
    :param save_graph_path: 出力ファイルパス
    :param style: 描写のスタイル(plt : defaultは'ggplot')
    :return:
    """
    column_name = x_column_name + ',' + y_column_name

    df = extract_data_as_pandas(db_pass, table_name, column_name, start_time, end_time)
    df = df.dropna()  # nullが入っている行は自動的に削除する
    plot_xy_graph(df, x_column_name, y_column_name, save_graph_path, style)


def plot_hist_from_db(db_pass, table_name, column_name, start_time, end_time, save_graph_path, style=default_style, bins=10, normed=True):
    u"""
    sqlite3のDBからtableのcolume_nameを指定して、範囲内のhistogramを作成する
    :param db_pass: sqlite3のDBパス
    :param table_name: DB内のtableのname
    :param column_name: 対象のcolumnのname
    :param start_time: 対象の時間(開始)
    :param end_time: 対象の時間(終了)
    :param save_graph_path: 出力ファイルパス
    :param style: 描写のスタイル(plt : defaultは'ggplot')
    :param bins: 棒を何本にするか
    :param normed: 正規化するか。ただし、複数同時にヒストグラムを作成すると縦軸が変になる
    :return:
    """

    plt.style.use(style)  # chart設定

    df = extract_data_as_pandas(db_pass, table_name, column_name, start_time, end_time)
    df = df.dropna()  # nullが入っている行は自動的に削除する
    df.plot.hist(bins=bins, normed=normed)
    plt.savefig(save_graph_path)
    print('Save picture to "' + str(save_graph_path) + '"')
    return
