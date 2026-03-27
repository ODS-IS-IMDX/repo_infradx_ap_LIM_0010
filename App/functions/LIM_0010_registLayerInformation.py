# © 2026 NTT DATA Japan Co., Ltd. & NTT InfraNet All Rights Reserved.

"""
LIM_0010_registLayerInformation.py

処理名:
    レイヤ情報登録

概要:
    ベクタレイヤマスタにレイヤ情報を登録した上で設備データ管理マスタDBにテーブルを作成する。
    また、設備小項目×最終断面種別単位で、認可パターンを最終断面認可マスタに登録する。

    【補足】
    本バッチの実行にはchardetライブラリ（バージョン: 5.2.0）が必要です。
    chardetがインストールされていない場合、文字コード確認でエラーとなりますので、事前にインストールしてください。

実行コマンド形式:
    python3 [バッチ格納先パス]/LIM_0010_registLayerInformation.py
    --filename=[ファイル名]
"""

import argparse
import csv
import os
import traceback
from datetime import datetime
from pathlib import Path

import chardet
from core.config_reader import read_config
from core.constants import Constants
from core.database import Database
from core.logger import LogManager
from core.secretProperties import SecretPropertiesSingleton
from core.validations import Validations
from util.checkProviderExistence import check_provider_existence
from util.getProviderId import get_provider_id

log_manager = LogManager()
logger = log_manager.get_logger("LIM_0010_レイヤ情報登録")
config = read_config(logger)

# 設定ファイルから取得
DDL_FOLDER = config["folderPass"]["ddl_folder"].strip()
LAYER_CSV_WORK_FOLDER = config["folderPass"]["layer_csv_work_folder"].strip()
FAC_SUBITEM_NAME = config["constant"]["fac_subitem_name"].strip()
GEOMETRY_TYPE = config["constant"]["geometry_type"].strip()
FINAL_CROSS_SECTION_TYPE = config["constant"]["final_cross_section_type"].strip()
AUTHORIZATION_PATTERN = config["constant"]["authorization_pattern"].strip()
LAYER_NAME = config["constant"]["layer_name"].strip()
LAYER_SUMMARY = config["constant"]["layer_summary"].strip()
START_DATE_OF_USE = config["constant"]["start_date_of_use"].strip()
END_DATE_OF_USE = config["constant"]["end_date_of_use"].strip()
SECRET_NAME = config["aws"]["secret_name"].strip()


# 起動パラメータを受け取る関数
def parse_args():
    try:
        # 完全一致のみ許可
        parser = argparse.ArgumentParser(allow_abbrev=False, exit_on_error=False)
        parser.add_argument("--filename", required=False)
        return parser.parse_args()
    except Exception as e:
        # コマンドライン引数の解析に失敗した場合
        logger.error("BPE0037", str(e.message))
        logger.process_error_end()


# 1. 入力値チェック
def validate_file_name(file_name):
    # 必須パラメータチェック
    if not file_name:
        logger.error("BPE0018", "ファイル名")
        logger.process_error_end()

    # 接頭辞が"layer_information_"であること
    if not Validations.is_prefix(file_name, Constants.PREFIX_LAYER_INFORMATION):
        logger.error("BPE0019", "ファイル名", file_name)
        logger.process_error_end()

    # 拡張子が".csv"であること
    if not Validations.is_suffix(file_name, Constants.SUFFIX_CSV):
        logger.error("BPE0019", "ファイル名", file_name)
        logger.process_error_end()

    # 接頭辞"layer_information_"と拡張子".csv"を除去し公益事業者・道路管理者コードとして使用
    provider_code = file_name[
        # fmt: off
        len(Constants.PREFIX_LAYER_INFORMATION):-len(Constants.SUFFIX_CSV)
        # fmt: on
    ]

    # 半角数字とハイフンのみで構成されていること
    if not Validations.is_digit_hyphen(provider_code):
        logger.error("BPE0019", "ファイル名", file_name)
        logger.process_error_end()

    # 桁数（1以上20以下）
    if not Validations.is_valid_length(provider_code, 1, 20):
        logger.error("BPE0019", "ファイル名", file_name)
        logger.process_error_end()

    # 入力値チェック完了後、公益事業者・道路管理者コードを返す
    return provider_code


# 2. CSVファイル存在確認
def check_csv_file_exists(file_path):
    # 対象のCSVファイルが存在するか
    if not file_path.is_file():
        logger.error("BPE0033", file_path)
        logger.process_error_end()


# 3. CSVファイル文字コード確認
def check_csv_encoding(file_path):
    # CSVファイルの文字コードがUTF-8であること
    try:
        with open(file_path, "rb") as f:
            rawdata = f.read()
            result = chardet.detect(rawdata)
            detected_encoding = result["encoding"]

        if not (detected_encoding.upper() == Constants.CHARACTER_ENCODING_UTF_8):
            logger.error("BPE0008", file_path)
            logger.process_error_end()
    except Exception:
        logger.error("BPE0007", file_path)
        logger.process_error_end()


# 6. CSVファイル読み込み（レイヤ情報リスト作成）
def read_csv(file_path):
    # CSVファイルを読み込みレイヤ情報リストを作成
    with open(
        file_path, mode="r", encoding=Constants.CHARACTER_ENCODING_UTF_8
    ) as csv_file:
        reader = csv.reader(csv_file)
        layer_information_list = [row for row in reader]
        return layer_information_list


# 7. ヘッダー項目チェック
def validate_header(header):
    # 全体の列数が8であること
    if not len(header) == 8:
        logger.error("BPE0010", "1", header)
        logger.process_error_end()

    # ヘッダーを展開
    (
        fac_subitem_name,
        geometry_type,
        final_cross_section_type,
        authorization_pattern,
        layer_name,
        layer_summary,
        start_date_of_use,
        end_date_of_use,
    ) = header

    # ヘッダーの列名が設定ファイルの値と一致すること
    # 設備小項目名
    if not fac_subitem_name == FAC_SUBITEM_NAME:
        logger.error("BPE0026", FAC_SUBITEM_NAME, "1", header)
        logger.process_error_end()
    # ジオメトリタイプ
    if not geometry_type == GEOMETRY_TYPE:
        logger.error("BPE0026", GEOMETRY_TYPE, "1", header)
        logger.process_error_end()
    # 最終断面種別
    if not final_cross_section_type == FINAL_CROSS_SECTION_TYPE:
        logger.error("BPE0026", FINAL_CROSS_SECTION_TYPE, "1", header)
        logger.process_error_end()
    # 認可パターン
    if not authorization_pattern == AUTHORIZATION_PATTERN:
        logger.error("BPE0026", AUTHORIZATION_PATTERN, "1", header)
        logger.process_error_end()
    # レイヤ名
    if not layer_name == LAYER_NAME:
        logger.error("BPE0026", LAYER_NAME, "1", header)
        logger.process_error_end()
    # レイヤ概要
    if not layer_summary == LAYER_SUMMARY:
        logger.error("BPE0026", LAYER_SUMMARY, "1", header)
        logger.process_error_end()
    # 利用開始年月日
    if not start_date_of_use == START_DATE_OF_USE:
        logger.error("BPE0026", START_DATE_OF_USE, "1", header)
        logger.process_error_end()
    # 利用終了年月日
    if not end_date_of_use == END_DATE_OF_USE:
        logger.error("BPE0026", END_DATE_OF_USE, "1", header)
        logger.process_error_end()


# 8. レイヤ情報リスト項目チェック
def validate_layer_information_rows(layer_information_list):

    # 認可パターンリスト
    authorization_pattern_list = []
    # 設備小項目名リスト
    fac_subitem_name_list = []

    # ヘッダーを除いたレイヤ情報リストのチェック
    for row_count, layer_infomation in enumerate(
        layer_information_list[1:], start=2
    ):  # ヘッダーを除く

        # 全体の列数が8であること
        if not len(layer_infomation) == 8:
            logger.error("BPE0010", row_count, layer_infomation)
            logger.process_error_end()

        # テンプレートを展開
        (
            fac_subitem_name,
            geometry_type,
            final_cross_section_type,
            authorization_pattern,
            layer_name,
            layer_summary,
            start_date_of_use,
            end_date_of_use,
        ) = layer_infomation

        # 設備小項目名チェック
        # 必須チェック
        if not Validations.is_required_for_csv(fac_subitem_name):
            logger.error("BPE0024", FAC_SUBITEM_NAME, row_count, layer_infomation)
            logger.process_error_end()

        # 桁数（1以上20以下）
        if not Validations.is_valid_length(fac_subitem_name, 1, 20):
            logger.error("BPE0026", FAC_SUBITEM_NAME, row_count, layer_infomation)
            logger.process_error_end()

        # ジオメトリタイプチェック
        # 必須チェック
        if not Validations.is_required_for_csv(geometry_type):
            logger.error("BPE0024", GEOMETRY_TYPE, row_count, layer_infomation)
            logger.process_error_end()

        # アンダースコア区切りで分割
        geometry_type_parts = geometry_type.split("_")
        if (
            # 桁数（1以上18以下）
            not Validations.is_valid_length(geometry_type, 1, 18)
            # 半角英字とアンダースコアのみで構成されているか
            or not Validations.is_al_underscore(geometry_type)
            # ジオメトリタイプリストのいずれかの値であること
            or not Validations.is_value_in_list(
                geometry_type_parts, Constants.GEOMETRY_TYPE_LIST
            )
        ):
            logger.error("BPE0026", GEOMETRY_TYPE, row_count, layer_infomation)
            logger.process_error_end()

        # 最終断面種別チェック
        # 必須チェック
        if not Validations.is_required_for_csv(final_cross_section_type):
            logger.error(
                "BPE0024", FINAL_CROSS_SECTION_TYPE, row_count, layer_infomation
            )
            logger.process_error_end()

        if (
            # 半角数字1文字で構成されているか
            not Validations.is_single_digit(final_cross_section_type)
            # 最終断面リストのいずれかの値であること
            or not Validations.is_value_in_list(
                int(final_cross_section_type), Constants.FINAL_CROSS_SECTION_LIST
            )
        ):
            logger.error(
                "BPE0026", FINAL_CROSS_SECTION_TYPE, row_count, layer_infomation
            )
            logger.process_error_end()

        # 認可パターンチェック
        # 必須チェック
        if not Validations.is_required_for_csv(authorization_pattern):
            logger.error("BPE0024", AUTHORIZATION_PATTERN, row_count, layer_infomation)
            logger.process_error_end()

        # 桁数（1以上30以下）
        if not Validations.is_valid_length(authorization_pattern, 1, 30):
            logger.error("BPE0026", AUTHORIZATION_PATTERN, row_count, layer_infomation)
            logger.process_error_end()

        # レイヤ名チェック
        # 必須チェック
        if not Validations.is_required_for_csv(layer_name):
            logger.error("BPE0024", LAYER_NAME, row_count, layer_infomation)
            logger.process_error_end()

        # 桁数（1以上50以下）
        if not Validations.is_valid_length(layer_name, 1, 50):
            logger.error("BPE0026", LAYER_NAME, row_count, layer_infomation)
            logger.process_error_end()

        # レイヤ概要チェック
        # 必須チェック
        if not Validations.is_required_for_csv(layer_summary):
            logger.error("BPE0024", LAYER_SUMMARY, row_count, layer_infomation)
            logger.process_error_end()

        # 桁数（1以上200以下）
        if not Validations.is_valid_length(layer_summary, 1, 200):
            logger.error("BPE0026", LAYER_SUMMARY, row_count, layer_infomation)
            logger.process_error_end()

        # 利用開始年月日チェック
        # 必須チェック
        if not Validations.is_required_for_csv(start_date_of_use):
            logger.error("BPE0024", START_DATE_OF_USE, row_count, layer_infomation)
            logger.process_error_end()

        # フォーマットがYYYYMMDD形式であるか
        # 存在する日付か
        if not Validations.is_date_format(start_date_of_use):
            logger.error("BPE0026", START_DATE_OF_USE, row_count, layer_infomation)
            logger.process_error_end()

        # 利用終了年月日チェック
        # 必須チェック
        if not Validations.is_required_for_csv(end_date_of_use):
            logger.error("BPE0024", END_DATE_OF_USE, row_count, layer_infomation)
            logger.process_error_end()

        if (
            # フォーマットがYYYYMMDD形式であるか
            # 存在する日付か
            not Validations.is_date_format(end_date_of_use)
            # 前後関係（利用終了年月日が利用開始年月日よりも未来の日付か）
            or not start_date_of_use < end_date_of_use
        ):
            logger.error("BPE0026", END_DATE_OF_USE, row_count, layer_infomation)
            logger.process_error_end()

        # 認可パターンリストに追加
        authorization_pattern_list.append(authorization_pattern)
        # 設備小項目名リストに追加
        fac_subitem_name_list.append(fac_subitem_name)

    # 配列の重複を削除
    authorization_pattern_list = list(dict.fromkeys(authorization_pattern_list))
    fac_subitem_name_list = list(dict.fromkeys(fac_subitem_name_list))

    return authorization_pattern_list, fac_subitem_name_list


# 9. 認可パターン存在確認
def check_authorization_pattern_exists(
    db_connection, db_mst_schema, authorization_pattern_list, logger
):
    # 認可パターンマスタに既存データが存在するか確認
    query = (
        f"SELECT (SELECT COUNT(*) FROM {db_mst_schema}."
        "mst_authorization_pattern WHERE authorization_pattern IN %s) = %s;"
    )
    result = Database.execute_query(
        db_connection,
        logger,
        query,
        (
            tuple(authorization_pattern_list),
            len(authorization_pattern_list),
        ),
        fetchone=True,
    )
    if not result:
        logger.error(
            "BPE0055", "認可パターンマスタ", "認可パターン", authorization_pattern_list
        )
        logger.process_error_end()


# 10. 設備小項目名存在確認
def check_fac_subitem_name_exists(
    db_connection, db_mst_schema, fac_subitem_name_list, logger
):
    # 設備小項目マスタに既存データが存在するか確認
    query = (
        f"SELECT (SELECT COUNT(*) FROM {db_mst_schema}."
        "mst_fac_subitem WHERE fac_subitem_name IN %s) = %s;"
    )
    result = Database.execute_query(
        db_connection,
        logger,
        query,
        (
            tuple(fac_subitem_name_list),
            len(fac_subitem_name_list),
        ),
        fetchone=True,
    )
    if not result:
        logger.error(
            "BPE0055", "設備小項目マスタ", "設備小項目名", fac_subitem_name_list
        )
        logger.process_error_end()


# 11. 認可パターンID取得
def get_authorization_pattern_codelist(
    db_connection, db_mst_schema, authorization_pattern_list, logger
):
    # 認可パターンマスタから認可パターン・認可パターンIDをJSONオブジェクト形式で取得
    query = (
        f"SELECT json_object_agg(authorization_pattern, "
        f"authorization_pattern_id) FROM {db_mst_schema}."
        "mst_authorization_pattern WHERE authorization_pattern IN %s;"
    )
    result = Database.execute_query(
        db_connection,
        logger,
        query,
        (tuple(authorization_pattern_list),),
        fetchone=True,
    )
    return result


# 12. 設備小項目ID・設備小項目英名取得
def get_fac_subitem_codelist(
    db_connection, db_mst_schema, fac_subitem_name_list, logger
):
    # 設備小項目マスタから設備小項目名・設備小項目ID・設備小項目英名をJSONオブジェクト形式で取得
    query = (
        f"SELECT json_object_agg(fac_subitem_name, "
        f"json_build_object('fac_subitem_id', fac_subitem_id, "
        f"'fac_subitem_eng', fac_subitem_eng)) "
        f"FROM {db_mst_schema}.mst_fac_subitem "
        f"WHERE fac_subitem_name IN %s;"
    )
    result = Database.execute_query(
        db_connection,
        logger,
        query,
        (tuple(fac_subitem_name_list),),
        fetchone=True,
    )
    return result


# 13. レイヤ情報リスト修正
def modify_layer_information_list(
    provider_id,
    layer_information_list,
    authorization_pattern_codelist,
    fac_subitem_codelist,
):
    # ジオメトリタイプリスト
    geometry_type_list = []
    # レイヤIDリスト
    layer_id_list = []
    # 最終断面認可リスト
    final_cross_section_authorization_list = []

    # 13-1. ヘッダー削除（0番目の配列を削除）
    layer_information_list_without_header = layer_information_list[1:]

    # 修正後レイヤ情報リスト
    modified_layer_information_list = []

    for layer_information in layer_information_list_without_header:
        # 各項目を取得
        (
            fac_subitem_name,
            geometry_type,
            final_cross_section_type,
            authorization_pattern,
            layer_name,
            layer_summary,
            start_date_of_use,
            end_date_of_use,
        ) = layer_information

        # 13-2. 認可パターンID置き換え
        authorization_pattern_id = authorization_pattern_codelist[authorization_pattern]

        # 13-3. 設備小項目ID置き換え
        fac_subitem_information = fac_subitem_codelist[fac_subitem_name]
        fac_subitem_id = fac_subitem_information["fac_subitem_id"]
        fac_subitem_eng = fac_subitem_information["fac_subitem_eng"]

        # 13-4. レイヤID追加
        # 最終断面種別に対応する文字列を取得
        final_cross_section_type_int = int(final_cross_section_type)
        if final_cross_section_type_int == Constants.FINAL_CROSS_SECTION_2D:
            final_cross_section_identifier = (
                Constants.FINAL_CROSS_SECTION_INTERMEDIATE_SUFFIX_2D
            )
        elif final_cross_section_type_int == Constants.FINAL_CROSS_SECTION_3D:
            final_cross_section_identifier = (
                Constants.FINAL_CROSS_SECTION_INTERMEDIATE_SUFFIX_3D
            )

        # ジオメトリタイプをアンダースコア区切りで分割してジオメトリタイプリストに追加
        geometry_type_list = geometry_type.split("_")

        for geometry_type in geometry_type_list:
            # レイヤID作成：[設備小項目英名]_[ジオメトリタイプ][最終断面識別子][公益事業者・道路管理者ID]
            layer_id = (
                f"{fac_subitem_eng}_{geometry_type}"
                f"{final_cross_section_identifier}{provider_id}"
            )

            # 修正後のレイヤ情報リストに追加
            # [設備小項目ID, ジオメトリタイプ, 最終断面種別, 認可パターンID,
            # レイヤID, レイヤ名, レイヤ概要, 利用開始年月日, 利用終了年月日]
            modified_layer_information = [
                fac_subitem_id,
                geometry_type,
                final_cross_section_type_int,
                authorization_pattern_id,
                layer_id,
                layer_name,
                layer_summary,
                start_date_of_use,
                end_date_of_use,
            ]
            modified_layer_information_list.append(modified_layer_information)

            # 13-5. レイヤIDリスト、最終断面認可リスト追加
            layer_id_list.append(layer_id)
            final_cross_section_authorization_list.append(
                (fac_subitem_id, provider_id, final_cross_section_type_int)
            )

    # 最終断面認可リストの重複を削除
    final_cross_section_authorization_list = list(
        dict.fromkeys(final_cross_section_authorization_list)
    )

    return (
        modified_layer_information_list,
        layer_id_list,
        final_cross_section_authorization_list,
    )


# 14. ベクタレイヤ既存データ確認
def check_vector_layer_exists(db_connection, db_mst_schema, layer_id_list, logger):
    # ベクタレイヤマスタに既存データが存在するか確認
    query = (
        f"SELECT layer_id FROM {db_mst_schema}.mst_vector_layer WHERE layer_id IN %s;"
    )
    result = Database.execute_query(
        db_connection,
        logger,
        query,
        (tuple(layer_id_list),),
        fetchall=True,
    )
    if result:
        logger.error("BPE0021", [t[0] for t in result])
        logger.process_error_end()


# 15. 最終断面認可既存データ確認
def check_final_cross_section_authorization_exists(
    db_connection, db_mst_schema, final_cross_section_authorization_list, logger
):
    # 最終断面認可マスタに既存データが存在するか確認
    query = (
        f"SELECT fac_subitem_id, provider_id, final_cross_section_type "
        f"FROM {db_mst_schema}.mst_final_cross_section_authorization "
        f"WHERE (fac_subitem_id, provider_id, final_cross_section_type) IN %s;"
    )
    result = Database.execute_query(
        db_connection,
        logger,
        query,
        (tuple(final_cross_section_authorization_list),),
        fetchall=True,
    )
    if result:
        logger.error("BPE0061", result)
        logger.process_error_end()


# 17. ベクタレイヤマスタ・最終断面認可マスタ登録
def insert_vector_layer_and_final_cross_section_authorization(
    db_connection, db_mst_schema, provider_id, layer_information_list, current_time
):
    # ベクタレイヤマスタにデータを登録
    vector_layer_query = (
        f"INSERT INTO {db_mst_schema}.mst_vector_layer "
        "(layer_id, layer_name, layer_summary, fac_subitem_id, "
        "final_cross_section_type, geometry_type, provider_id, "
        "start_date_of_use, end_date_of_use, created_by, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    # 最終断面認可マスタにデータを登録
    final_cross_section_authorization_query = (
        f"INSERT INTO {db_mst_schema}.mst_final_cross_section_authorization "
        "(fac_subitem_id, provider_id, final_cross_section_type, "
        "authorization_pattern_id, created_by, created_at) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )

    # 一意制約チェックタプルリスト
    unique_tuple_list = []

    with db_connection as conn:
        for layer_information in layer_information_list:
            # 各項目を展開
            (
                fac_subitem_id,
                geometry_type,
                final_cross_section_type_int,
                authorization_pattern_id,
                layer_id,
                layer_name,
                layer_summary,
                start_date_of_use,
                end_date_of_use,
            ) = layer_information

            Database.execute_query_no_commit(
                conn,
                logger,
                vector_layer_query,
                (
                    layer_id,
                    layer_name,
                    layer_summary,
                    fac_subitem_id,
                    final_cross_section_type_int,
                    geometry_type,
                    provider_id,
                    start_date_of_use,
                    end_date_of_use,
                    "system",
                    current_time,
                ),
            )

            # 一意制約チェックタプル
            unique_tuple = (
                fac_subitem_id,
                provider_id,
                final_cross_section_type_int,
            )

            # 既に登録済みの場合はスキップ
            if unique_tuple in unique_tuple_list:
                continue

            Database.execute_query_no_commit(
                conn,
                logger,
                final_cross_section_authorization_query,
                (
                    fac_subitem_id,
                    provider_id,
                    final_cross_section_type_int,
                    authorization_pattern_id,
                    "system",
                    current_time,
                ),
            )

            # 一意制約チェックタプルリストに追加
            unique_tuple_list.append(unique_tuple)

        # 全ての登録処理が成功した場合のみコミット
        conn.commit()


# 18. 登録済みレイヤID・レイヤ名出力
def log_registered_vector_layer(layer_information_list):
    # レイヤセットリスト
    layer_set_list = []

    for layer_information in layer_information_list:
        layer_id = layer_information[4]
        layer_name = layer_information[5]
        layer_set = f"[レイヤID:{layer_id}, レイヤ名:{layer_name}]"
        layer_set_list.append(layer_set)

    logger.info("BPI0003", layer_set_list)


# 19. 設備データ管理マスタDB DDLファイル存在確認
def check_facility_data_management_ddl_exists(fac_subitem_codelist, sql_file_path):
    # sqlファイルリストを作成（設備小項目英名 + .sql）
    sql_file_list = [
        f"{fac_subitem_information['fac_subitem_eng']}.sql"
        for fac_subitem_information in fac_subitem_codelist.values()
    ]

    # 非存在ファイル名リスト
    non_exist_file_list = []

    # sqlファイルの存在確認
    for sql_file_name in sql_file_list:
        file_path = sql_file_path / sql_file_name
        if not file_path.is_file():
            non_exist_file_list.append(sql_file_name)

    if non_exist_file_list:
        logger.error("BPE0004", "設備データ管理マスタDB", non_exist_file_list)
        logger.process_error_end()
    return sql_file_list


# 20. 設備データ管理マスタDB テーブル作成
def create_facility_data_management_tables(
    db_connection, provider_id, sql_file_path, sql_file_list
):
    # 未実行設備小項目英名リスト
    unexecuted_fac_subitem_eng_list = []

    # 警告フラグと異常フラグを初期化
    warning_flag = False
    error_flag = False

    # sqlファイルリスト内のsqlファイルよりDDLを実行
    for sql_file_name in sql_file_list:

        # 20-1. 文字列置換
        file_path = sql_file_path / sql_file_name

        try:
            with open(file_path, "r", encoding="utf-8") as sql_file:
                query = sql_file.read()
            # {provider_id}を実際の公益事業者・道路管理者IDに置換
            query = query.replace("{provider_id}", str(provider_id))
        except Exception:
            unexecuted_fac_subitem_eng_list.append(sql_file_name[:-4])
            continue

        # 20-2. DDL実行
        try:
            Database.execute_query(
                db_connection,
                logger,
                query,
                commit=True,
                raise_exception=True,
            )
        except Exception:
            unexecuted_fac_subitem_eng_list.append(sql_file_name[:-4])

    # 20-3. DDL失敗時ログ出力
    if unexecuted_fac_subitem_eng_list:
        # 未実行sqlファイルの文字列を作成
        unexecuted_sql_file = (
            f"[設備小項目英名:{unexecuted_fac_subitem_eng_list}, "
            f"公益事業者・道路管理者ID:{provider_id}]"
        )

        # すべてのDDL実行に失敗した場合
        if len(unexecuted_fac_subitem_eng_list) == len(sql_file_list):
            logger.error("BPE0058", unexecuted_sql_file)
            error_flag = True
        # 一部のDDL実行に失敗した場合
        else:
            logger.warning("BPW0025", unexecuted_sql_file)
            warning_flag = True
    return warning_flag, error_flag


# 21. CSVファイル削除
def delete_csv_file(file_path, warning_flag):
    # CSVファイルの削除
    try:
        os.remove(file_path)
    except Exception:
        # 削除に失敗した場合、警告ログを出力
        logger.warning("BPW0026", str(file_path))
        warning_flag = True
    return warning_flag


# 22. 終了コード返却
def determine_exit_code(warning_flag, error_flag):
    # 異常フラグがTRUEの場合
    if error_flag:
        # 異常終了
        logger.process_error_end()
    # 警告フラグがTRUEの場合かつ異常フラグがFALSEの場合
    elif warning_flag:
        # 警告終了
        logger.process_warning_end()
    # 両方FALSEの場合
    else:
        # 正常終了
        logger.process_normal_end()


# メイン処理
# レイヤ情報登録
def main():

    try:
        # 開始ログ出力
        logger.process_start()

        # 起動パラメータの取得
        file_name = parse_args().filename

        # 1. 入力値チェック
        provider_code = validate_file_name(file_name)

        # CSVファイルのパスを設定
        file_path = Path(LAYER_CSV_WORK_FOLDER) / file_name

        # 2. CSVファイル存在確認
        check_csv_file_exists(file_path)

        # 3. CSVファイル文字コード確認
        check_csv_encoding(file_path)

        # secret_propsにAWS Secrets Managerの値を格納
        secret_props = SecretPropertiesSingleton(SECRET_NAME, config, logger)

        # シークレットからマスタ管理スキーマ名を取得
        db_mst_schema = secret_props.get("db_mst_schema")

        # DB接続を取得
        db_connection = Database.get_mstdb_connection(logger)

        # 4. 公益事業者・道路管理者マスタ既存データ確認
        check_provider_existence(db_connection, db_mst_schema, provider_code, logger)

        # 5. 公益事業者・道路管理者ID取得
        provider_id = get_provider_id(
            db_connection, db_mst_schema, provider_code, logger
        )

        # 6. CSVファイル読み込み（レイヤ情報リスト作成）
        layer_information_list = read_csv(file_path)

        # 7. ヘッダー項目チェック
        validate_header(layer_information_list[0])

        # 8. レイヤ情報リスト項目チェック
        authorization_pattern_list, fac_subitem_name_list = (
            validate_layer_information_rows(layer_information_list)
        )

        # 9. 認可パターン存在確認
        check_authorization_pattern_exists(
            db_connection, db_mst_schema, authorization_pattern_list, logger
        )

        # 10. 設備小項目名存在確認
        check_fac_subitem_name_exists(
            db_connection, db_mst_schema, fac_subitem_name_list, logger
        )

        # 11. 認可パターンID取得
        authorization_pattern_codelist = get_authorization_pattern_codelist(
            db_connection, db_mst_schema, authorization_pattern_list, logger
        )

        # 12. 設備小項目ID・設備小項目英名取得
        fac_subitem_codelist = get_fac_subitem_codelist(
            db_connection, db_mst_schema, fac_subitem_name_list, logger
        )

        # 13. レイヤ情報リスト修正
        (
            layer_information_list,
            layer_id_list,
            final_cross_section_authorization_list,
        ) = modify_layer_information_list(
            provider_id,
            layer_information_list,
            authorization_pattern_codelist,
            fac_subitem_codelist,
        )

        # 14. ベクタレイヤ既存データ確認
        check_vector_layer_exists(db_connection, db_mst_schema, layer_id_list, logger)

        # 15. 最終断面認可既存データ確認
        check_final_cross_section_authorization_exists(
            db_connection,
            db_mst_schema,
            final_cross_section_authorization_list,
            logger,
        )

        # 16. 現在日時取得
        current_time = datetime.now()

        # 17. ベクタレイヤマスタ・最終断面認可マスタ登録
        insert_vector_layer_and_final_cross_section_authorization(
            db_connection,
            db_mst_schema,
            provider_id,
            layer_information_list,
            current_time,
        )

        # 18. 登録済みレイヤID・レイヤ名出力
        log_registered_vector_layer(layer_information_list)

        # sqlファイルパス
        sql_file_path = Path(DDL_FOLDER)

        # 19. 設備データ管理マスタDB DDLファイル存在確認
        sql_file_list = check_facility_data_management_ddl_exists(
            fac_subitem_codelist, sql_file_path
        )

        # 20. 設備データ管理マスタDB テーブル作成
        warning_flag, error_flag = create_facility_data_management_tables(
            db_connection, provider_id, sql_file_path, sql_file_list
        )

        # 21. CSVファイル削除
        warning_flag = delete_csv_file(file_path, warning_flag)

        # 22. 終了コード返却
        determine_exit_code(warning_flag, error_flag)

    except Exception:
        logger.error("BPE0009", traceback.format_exc())
        logger.process_error_end()


if __name__ == "__main__":
    main()
