import re
import zipfile
from datetime import date, timedelta
from io import BytesIO
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import openpyxl
import pandas as pd
import streamlit as st
from openpyxl.utils import get_column_letter
from openpyxl.workbook import Workbook


STUDIO9_KEYWORD = "STUDIO 9"
PEACH_BAG_KEYWORD = "Peach Tote Bag"


SO_EXCLUDE_KEYWORDS = [
    "包袋",
    "胸针",
    "贺卡",
    "包装服务",
    "手写贺卡",
    "纸袋",
]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _first_existing_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = set(map(str, df.columns))
    for c in candidates:
        if c in cols:
            return c
    # 兜底：去空格后匹配
    norm_map = {str(c).strip(): str(c).strip() for c in df.columns}
    for c in candidates:
        if str(c).strip() in norm_map:
            return norm_map[str(c).strip()]
    return None


def _ensure_numeric(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)


def _contains_keyword(series: pd.Series, keywords: List[str]) -> pd.Series:
    pattern = "|".join(re.escape(k) for k in keywords if k)
    if not pattern:
        return pd.Series([False] * len(series), index=series.index)
    return series.astype(str).str.contains(pattern, case=False, na=False, regex=True)


def _clean_th_key(external_single_no: object) -> str:
    s = "" if external_single_no is None else str(external_single_no).strip()
    if not s:
        return ""
    # TH开头且后接至少11位数字：TH + 11 digits
    if s.startswith("TH"):
        digits = re.sub(r"\D+", "", s[2:])
        return digits[:11] if len(digits) >= 11 else digits
    # 其他情况：抓取首次11位连续数字
    m = re.search(r"(\d{11})", s)
    if m:
        return m.group(1)
    digits = re.sub(r"\D+", "", s)
    return digits[-11:] if len(digits) >= 11 else digits


def _parse_first_order_id(x: object) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    s = str(x).strip()
    if not s:
        return ""
    # 可能存在多个订单号：逗号/分号/中文逗号/空格/换行等分隔
    tokens = re.split(r"[,\s;，/|]+", s)
    tokens = [t.strip() for t in tokens if t.strip()]
    return tokens[0] if tokens else s


def _shop_bucket(shop: str) -> str:
    s = "" if shop is None or (isinstance(shop, float) and np.isnan(shop)) else str(shop).strip()
    if not s or s == "N/A" or s == "N／A":
        return "HAY-Tmall"
    if any(k in s for k in ["小红书", "HAY旗舰店", "RED"]):
        return "RED"
    if STUDIO9_KEYWORD in s:
        return "JD-STUDIO 9"
    if "天猫" in s or "Tmall" in s or "京东" in s:
        return "HAY-Tmall"
    # 兜底：按你的要求默认天猫/HAY-Tmall
    return "HAY-Tmall"


def _build_aux_map(aux_df: Optional[pd.DataFrame]) -> Dict[str, Tuple[str, str]]:
    """
    返回：前6位编码 -> (主计量单位, 辅计量单位)
    不保证辅助表字段名完全一致，因此做容错猜测。
    """
    if aux_df is None or aux_df.empty:
        return {}

    aux_df = _normalize_columns(aux_df)

    code_col = _first_existing_col(aux_df, ["匹配物料编码", "物料编码", "编码", "物料代码", "电商系统物料编码"])
    main_unit_col = _first_existing_col(aux_df, ["主计量单位", "主单位", "主计量", "主计量单位编码"])
    sub_unit_col = _first_existing_col(aux_df, ["辅计量单位", "辅单位", "辅计量", "辅计量单位编码"])

    # 允许辅助表只有一个单位列时
    if not main_unit_col:
        main_unit_col = _first_existing_col(aux_df, ["计量单位", "单位"]) or None
    if not sub_unit_col:
        sub_unit_col = main_unit_col

    if not code_col or not main_unit_col:
        return {}

    _ensure_numeric(aux_df, code_col)

    result: Dict[str, Tuple[str, str]] = {}
    for _, r in aux_df.iterrows():
        c = r.get(code_col, "")
        if pd.isna(c):
            continue
        c_str = str(int(c)) if _is_int_like(c) else str(c).strip()
        c_str = re.sub(r"\.0+$", "", c_str)
        if not c_str:
            continue
        prefix6 = c_str[:6]
        main_u = r.get(main_unit_col, None)
        sub_u = r.get(sub_unit_col, None) if sub_unit_col else main_u
        main_u = "PCS" if main_u is None or (isinstance(main_u, float) and np.isnan(main_u)) or str(main_u).strip() == "" else str(main_u).strip()
        sub_u = main_u if sub_u is None or (isinstance(sub_u, float) and np.isnan(sub_u)) or str(sub_u).strip() == "" else str(sub_u).strip()
        result[prefix6] = (main_u, sub_u)
    return result


def _is_int_like(x: object) -> bool:
    try:
        if isinstance(x, (int, np.integer)):
            return True
        if isinstance(x, float) and float(x).is_integer():
            return True
    except Exception:
        return False
    return False


def _enrich_so_df(
    so1_df: pd.DataFrame,
    so2_df: pd.DataFrame,
    aux_map: Dict[str, Tuple[str, str]],
    debug: bool = False,
) -> pd.DataFrame:
    so1_df = _normalize_columns(so1_df)
    so2_df = _normalize_columns(so2_df)

    so1_df = so1_df.copy()
    so2_df = so2_df.copy()

    key1 = _first_existing_col(so1_df, ["出仓单号", "出仓单", "出库单号", "订单号", "订单编号"])
    key2 = _first_existing_col(so2_df, ["外部单号", "外部订单号", "订单号", "单号"])
    oms_col = _first_existing_col(so2_df, ["OMS"])

    if not key1 or not key2 or not oms_col:
        if debug:
            st.warning(f"SO无法定位合并键：key1={key1}, key2={key2}, oms_col={oms_col}")
        return pd.DataFrame()

    # Pandas merge 在不同数据类型（object/int/float）上可能触发 ValueError，
    # 因此在合并前将 join key 统一转为“去空格字符串”。
    so1_df[key1] = so1_df[key1].astype(str).str.strip().replace({"nan": "", "NaN": "", "None": ""})
    so2_df[key2] = so2_df[key2].astype(str).str.strip().replace({"nan": "", "NaN": "", "None": ""})

    # 仅聚水潭发货明细（宽容：contains，避免不可见空格导致等号匹配为空）
    so2_filt = so2_df.loc[so2_df[oms_col].astype(str).str.contains("聚水潭", na=False)].copy()
    if debug:
        st.write(f"SO：so1_rows={len(so1_df)}, so2_rows={len(so2_df)}, so2_filt_rows={len(so2_filt)}")
    if so2_filt.empty:
        return pd.DataFrame()

    merged = so1_df.merge(
        so2_filt,
        left_on=key1,
        right_on=key2,
        how="inner",
        suffixes=("_底1", "_底2"),
    )
    if merged.empty:
        if debug:
            st.write("SO：merge后为空（inner join 找不到匹配单号）")
        return pd.DataFrame()

    # 必备字段猜测
    qty_col = _first_existing_col(merged, ["实发数量", "数量"])
    amount_col = _first_existing_col(merged, ["实发金额", "金额"])
    ship_income_col = _first_existing_col(merged, ["运费收入分摊", "运费收入分摊金额", "运费分摊"])
    ship_fee_col = _first_existing_col(merged, ["运费金额", "运费"])
    sku_name_col = _first_existing_col(merged, ["商品简称", "商品名称", "商品描述"])
    sku_code_col = _first_existing_col(merged, ["商品编码", "物料编码", "SKU编码", "电商系统物料编码"])
    shop_col = _first_existing_col(merged, ["店铺", "店铺名称", "平台店铺"])
    order_col = key1
    online_order_col = _first_existing_col(merged, ["线上订单号", "客户订单号", "线上订单", "订单号线上"])

    if not qty_col or not amount_col:
        if debug:
            st.warning(f"SO无法定位金额/数量列：qty_col={qty_col}, amount_col={amount_col}")
        return pd.DataFrame()

    # 剔除非保仓：实发数量==0 的数据剔除
    _ensure_numeric(merged, qty_col)
    merged = merged.loc[merged[qty_col] != 0].copy()
    if debug:
        st.write(f"SO：实发数量!=0后 rows={len(merged)}")
    if merged.empty:
        return pd.DataFrame()

    if ship_income_col and amount_col:
        _ensure_numeric(merged, amount_col)
        _ensure_numeric(merged, ship_income_col)
        merged["实际支付金额"] = pd.to_numeric(merged[amount_col], errors="coerce").fillna(0) - pd.to_numeric(merged[ship_income_col], errors="coerce").fillna(0)
    else:
        # 缺少运费分摊时，直接等于实发金额
        merged["实际支付金额"] = pd.to_numeric(merged[amount_col], errors="coerce").fillna(0)
        if not ship_income_col:
            merged["运费收入分摊"] = 0

    if not ship_income_col:
        ship_income_col = "运费收入分摊"
        merged[ship_income_col] = 0
    else:
        merged[ship_income_col] = pd.to_numeric(merged[ship_income_col], errors="coerce").fillna(0)

    # 排除商品
    if sku_name_col:
        mask_exclude = _contains_keyword(merged[sku_name_col], SO_EXCLUDE_KEYWORDS)
        merged = merged.loc[~mask_exclude].copy()
        if debug:
            st.write(f"SO：剔除商品关键词后 rows={len(merged)}")
    if merged.empty:
        return pd.DataFrame()

    # 京东特殊规则：STUDIO 9 且单笔总金额 >= 799，Peach Tote Bag 强制设为0
    if shop_col and sku_name_col:
        merged[shop_col] = merged[shop_col].astype(str)
        merged["__是否STUDIO9__"] = merged[shop_col].str.contains(STUDIO9_KEYWORD, na=False)
        merged["__是否PeachBag__"] = merged[sku_name_col].astype(str).str.contains(PEACH_BAG_KEYWORD, na=False)

        order_total = merged.groupby(order_col)["实际支付金额"].sum()
        eligible_orders = set(order_total.loc[order_total >= 799].index.tolist())

        adj_mask = merged["__是否STUDIO9__"] & merged["__是否PeachBag__"] & merged[order_col].isin(eligible_orders)
        if adj_mask.any():
            # 将该包袋金额及运费分摊置0，避免出现实际支付金额为负
            merged.loc[adj_mask, amount_col] = 0
            if ship_income_col and ship_income_col in merged.columns:
                merged.loc[adj_mask, ship_income_col] = 0
            elif "运费收入分摊" in merged.columns:
                merged.loc[adj_mask, "运费收入分摊"] = 0
            merged.loc[adj_mask, "实际支付金额"] = 0
        merged.drop(columns=["__是否STUDIO9__", "__是否PeachBag__"], errors="ignore", inplace=True)

    # 运费H101切分
    if ship_fee_col:
        _ensure_numeric(merged, ship_fee_col)
        fee_mask = pd.to_numeric(merged[ship_fee_col], errors="coerce").fillna(0) > 0
        if fee_mask.any():
            h101_rows = merged.loc[fee_mask].copy()
            h101_rows[qty_col] = 1
            h101_rows["运费收入分摊"] = 0
            h101_rows["实际支付金额"] = pd.to_numeric(h101_rows[ship_fee_col], errors="coerce").fillna(0)
            # 商品编码切分：写入到“原始商品编码列”，避免后续 rename 覆盖 H101 设置
            if sku_code_col and sku_code_col in h101_rows.columns:
                h101_rows[sku_code_col] = "H101"
            else:
                h101_rows["商品编码"] = "H101"
            # 保留并追加
            merged = pd.concat([merged, h101_rows], ignore_index=True)

    # 输出统一列
    if not sku_code_col:
        merged["商品编码"] = ""
        sku_code_col = "商品编码"
    else:
        merged.rename(columns={sku_code_col: "商品编码"}, inplace=True)

    merged.rename(columns={qty_col: "实发数量"}, inplace=True)
    merged.rename(columns={order_col: "订单号"}, inplace=True)
    if shop_col and shop_col in merged.columns:
        merged["店铺"] = merged[shop_col]
    else:
        merged["店铺"] = "N/A"

    if online_order_col and online_order_col in merged.columns:
        merged["线上订单号"] = merged[online_order_col].apply(_parse_first_order_id)
    else:
        merged["线上订单号"] = merged["订单号"].apply(_parse_first_order_id)

    # 运费收入分摊列确保存在
    if ship_income_col not in merged.columns:
        merged["运费收入分摊"] = 0

    # 不强依赖：后续报表只用“商品编码/实发数量/实际支付金额/线上订单号/店铺/订单号”
    return merged[["店铺", "商品编码", "实发数量", "实际支付金额", "线上订单号", "订单号"]].copy()


def _enrich_rt_df(
    rt3_df: pd.DataFrame,
    rt4_df: pd.DataFrame,
    aux_map: Dict[str, Tuple[str, str]],
    debug: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    返回：(rt_main_df, rt_nonbao_df)
    rt_main_df: 实发数量 != 0
    rt_nonbao_df: 实发数量 == 0
    """
    rt3_df = _normalize_columns(rt3_df)
    rt4_df = _normalize_columns(rt4_df)

    oms_col = _first_existing_col(rt4_df, ["OMS"])
    external_key = _first_existing_col(rt4_df, ["外部单号", "外部订单号", "单号"])
    after_key = _first_existing_col(rt3_df, ["售后单号", "售后订单号", "单号"])

    if not oms_col or not external_key or not after_key:
        if debug:
            st.warning(f"RT无法定位合并键：oms_col={oms_col}, external_key={external_key}, after_key={after_key}")
        return pd.DataFrame(), pd.DataFrame()

    # 同样对 join key 做类型统一（字符串），避免 merge key dtype 不兼容导致 ValueError
    rt3_df[after_key] = rt3_df[after_key].astype(str).str.strip().replace({"nan": "", "NaN": "", "None": ""})
    rt4_df[external_key] = rt4_df[external_key].astype(str).str.strip().replace({"nan": "", "NaN": "", "None": ""})

    # 仅聚水潭收货明细（宽容：contains，避免不可见空格导致等号匹配为空）
    rt4_filt = rt4_df.loc[rt4_df[oms_col].astype(str).str.contains("聚水潭", na=False)].copy()
    if debug:
        st.write(f"RT：rt3_rows={len(rt3_df)}, rt4_rows={len(rt4_df)}, rt4_filt_rows={len(rt4_filt)}")
    if rt4_filt.empty:
        return pd.DataFrame(), pd.DataFrame()

    rt4_filt["__THKey__"] = rt4_filt[external_key].apply(_clean_th_key).astype(str).str.strip()
    rt4_filt = rt4_filt.loc[rt4_filt["__THKey__"] != ""].copy()
    if debug:
        st.write(f"RT：THKey!=空后 rows={len(rt4_filt)}")
    if rt4_filt.empty:
        return pd.DataFrame(), pd.DataFrame()

    merged = rt3_df.merge(
        rt4_filt,
        left_on=after_key,
        right_on="__THKey__",
        how="inner",
        suffixes=("_底3", "_底4"),
    )
    if merged.empty:
        if debug:
            st.write("RT：merge后为空（inner join 找不到匹配TH键/售后单号）")
        return pd.DataFrame(), pd.DataFrame()

    qty_col = _first_existing_col(merged, ["实发数量", "数量"])
    amount_col = _first_existing_col(merged, ["实发金额", "金额"])
    ship_income_col = _first_existing_col(merged, ["运费收入分摊", "运费收入分摊金额", "运费分摊"])
    ship_fee_col = _first_existing_col(merged, ["运费金额", "运费"])
    sku_name_col = _first_existing_col(merged, ["商品简称", "商品名称", "商品描述"])
    sku_code_col = _first_existing_col(merged, ["商品编码", "物料编码", "SKU编码", "电商系统物料编码"])
    shop_col = _first_existing_col(merged, ["店铺", "店铺名称", "平台店铺"])
    online_order_col = _first_existing_col(merged, ["线上订单号", "客户订单号", "线上订单", "订单号线上", "线上订单编号"])

    if not qty_col or not amount_col:
        if debug:
            st.warning(f"RT无法定位金额/数量列：qty_col={qty_col}, amount_col={amount_col}")
        return pd.DataFrame(), pd.DataFrame()

    _ensure_numeric(merged, qty_col)
    _ensure_numeric(merged, amount_col)

    # 实际支付金额
    if ship_income_col:
        _ensure_numeric(merged, ship_income_col)
        merged["实际支付金额"] = pd.to_numeric(merged[amount_col], errors="coerce").fillna(0) - pd.to_numeric(merged[ship_income_col], errors="coerce").fillna(0)
    else:
        merged["实际支付金额"] = pd.to_numeric(merged[amount_col], errors="coerce").fillna(0)
        merged["运费收入分摊"] = 0
        ship_income_col = "运费收入分摊"

    # 商品排除
    if sku_name_col:
        mask_exclude = _contains_keyword(merged[sku_name_col], SO_EXCLUDE_KEYWORDS)
        merged = merged.loc[~mask_exclude].copy()
        if debug:
            st.write(f"RT：剔除商品关键词后 rows={len(merged)}")
    if merged.empty:
        return pd.DataFrame(), pd.DataFrame()

    # 京东特殊规则同SO（STUDIO 9 & 单笔>=799 & Peach Tote Bag置0）
    if shop_col and sku_name_col:
        merged[shop_col] = merged[shop_col].astype(str)
        merged["__是否STUDIO9__"] = merged[shop_col].str.contains(STUDIO9_KEYWORD, na=False)
        merged["__是否PeachBag__"] = merged[sku_name_col].astype(str).str.contains(PEACH_BAG_KEYWORD, na=False)

        order_col = after_key
        order_total = merged.groupby(order_col)["实际支付金额"].sum()
        eligible_orders = set(order_total.loc[order_total >= 799].index.tolist())
        adj_mask = merged["__是否STUDIO9__"] & merged["__是否PeachBag__"] & merged[order_col].isin(eligible_orders)
        if adj_mask.any():
            merged.loc[adj_mask, amount_col] = 0
            if ship_income_col and ship_income_col in merged.columns:
                merged.loc[adj_mask, ship_income_col] = 0
            elif "运费收入分摊" in merged.columns:
                merged.loc[adj_mask, "运费收入分摊"] = 0
            merged.loc[adj_mask, "实际支付金额"] = 0
        merged.drop(columns=["__是否STUDIO9__", "__是否PeachBag__"], errors="ignore", inplace=True)

    # 运费H101切分
    if ship_fee_col:
        _ensure_numeric(merged, ship_fee_col)
        fee_mask = pd.to_numeric(merged[ship_fee_col], errors="coerce").fillna(0) > 0
        if fee_mask.any():
            h101_rows = merged.loc[fee_mask].copy()
            h101_rows[qty_col] = 1
            if ship_income_col in h101_rows.columns:
                h101_rows[ship_income_col] = 0
            h101_rows["实际支付金额"] = pd.to_numeric(h101_rows[ship_fee_col], errors="coerce").fillna(0)
            # 商品编码切分：写入到“原始商品编码列”，避免后续 rename 覆盖 H101 设置
            if sku_code_col and sku_code_col in h101_rows.columns:
                h101_rows[sku_code_col] = "H101"
            else:
                h101_rows["商品编码"] = "H101"
            merged = pd.concat([merged, h101_rows], ignore_index=True)

    if not sku_code_col:
        merged["商品编码"] = ""
        sku_code_col = "商品编码"
    else:
        merged.rename(columns={sku_code_col: "商品编码"}, inplace=True)

    merged.rename(columns={qty_col: "实发数量"}, inplace=True)
    merged.rename(columns={after_key: "订单号"}, inplace=True)
    if shop_col and shop_col in merged.columns:
        merged["店铺"] = merged[shop_col]
    else:
        merged["店铺"] = "N/A"

    if online_order_col and online_order_col in merged.columns:
        merged["线上订单号"] = merged[online_order_col].apply(_parse_first_order_id)
    else:
        merged["线上订单号"] = merged["订单号"].apply(_parse_first_order_id)

    if "实际支付金额" not in merged.columns:
        merged["实际支付金额"] = 0

    # 输出统一列
    base = merged[["店铺", "商品编码", "实发数量", "实际支付金额", "线上订单号", "订单号"]].copy()
    nonbao_df = base.loc[base["实发数量"] == 0].copy()
    main_df = base.loc[base["实发数量"] != 0].copy()
    return main_df, nonbao_df


def _compute_report_rows(
    df: pd.DataFrame,
    aux_map: Dict[str, Tuple[str, str]],
    is_return: bool,
) -> List[List[object]]:
    """
    返回给 openpyxl 的每行数据（不含表头），顺序严格为：
    [电商系统物料编码, 辅助自由项编码1, 主计量单位, 辅计量单位, 数量, 税率, 价税合计, 无税金额, 税额, 含税单价, 无税单价, 单品折扣, 整单折扣, 客户订单号, 是否赠品, 发货仓库编码]
    """
    if df is None or df.empty:
        return []

    df = df.copy()

    # 强制类型
    if "实发数量" in df.columns:
        _ensure_numeric(df, "实发数量")
    else:
        df["实发数量"] = 0
    if "实际支付金额" in df.columns:
        _ensure_numeric(df, "实际支付金额")
    else:
        df["实际支付金额"] = 0

    # 为避免除零：税/单价字段先按0处理
    qty = pd.to_numeric(df["实发数量"], errors="coerce").fillna(0)
    price = pd.to_numeric(df["实际支付金额"], errors="coerce").fillna(0)

    if is_return:
        qty = -qty
        price = -price

    ex_tax = (price / 1.13).round(2)
    tax = (price - ex_tax).round(2)
    with np.errstate(divide="ignore", invalid="ignore"):
        tax_incl_unit = np.where(qty != 0, (price / qty).round(4), 0.0)
        tax_excl_unit = np.where(qty != 0, (ex_tax / qty).round(4), 0.0)

    df["__qty__"] = qty
    df["__price__"] = price
    df["__ex_tax__"] = ex_tax
    df["__tax__"] = tax
    df["__tax_incl_unit__"] = tax_incl_unit
    df["__tax_excl_unit__"] = tax_excl_unit

    rows: List[List[object]] = []
    for i in range(len(df)):
        r = df.iloc[i]
        sku_code = "" if pd.isna(r.get("商品编码", "")) else str(r.get("商品编码", "")).strip()
        if "00" in sku_code and len(sku_code) > 6:
            main_code = sku_code[:6]
            aux_code = sku_code[6:]
        else:
            main_code = sku_code
            aux_code = ""

        prefix6 = main_code[:6] if len(main_code) >= 6 else main_code
        main_u, sub_u = aux_map.get(prefix6, ("PCS", "PCS"))
        if not main_u:
            main_u = "PCS"
        if not sub_u:
            sub_u = main_u

        quantity = r["__qty__"]
        amount_incl = r["__price__"]
        amount_excl = r["__ex_tax__"]
        amount_tax = r["__tax__"]
        unit_incl = r["__tax_incl_unit__"]
        unit_excl = r["__tax_excl_unit__"]

        client_order = _parse_first_order_id(r.get("线上订单号", ""))

        rows.append(
            [
                main_code,  # 电商系统物料编码
                aux_code,  # 辅助自由项编码1
                main_u,  # 主计量单位
                sub_u,  # 辅计量单位
                float(quantity),  # 数量
                "13.00",  # 税率
                float(amount_incl),  # 价税合计
                float(amount_excl),  # 无税金额
                float(amount_tax),  # 税额
                float(unit_incl),  # 含税单价
                float(unit_excl),  # 无税单价
                "100%",  # 单品折扣
                "100%",  # 整单折扣
                client_order,  # 客户订单号
                "否",  # 是否赠品
                "2107",  # 发货仓库编码
            ]
        )
    return rows


def _dataframe_to_excel_bytes(
    df: pd.DataFrame,
    yesterday: date,
    aux_map: Dict[str, Tuple[str, str]],
    is_return: bool,
) -> BytesIO:
    yesterday_slash = yesterday.strftime("%Y/%m/%d")
    yesterday_ymd = yesterday.strftime("%Y%m%d")

    output = BytesIO()
    wb: Workbook = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet1"

    # 表头：第1-2行
    ws["A1"] = "单据日期"
    ws["B1"] = "含税总金额"
    ws["C1"] = "交易类型"
    ws["D1"] = "流程类型"
    ws["E1"] = "收款协议"
    ws["F1"] = "部门编码"
    ws["G1"] = "客户编码"

    ws["A2"] = yesterday_slash
    ws["C2"] = "Cxx-TM02"
    ws["D2"] = "30-Cxx-04"
    ws["E2"] = "S003"
    ws["F2"] = "200101"
    ws["G2"] = "9701"

    # 第3行留空：默认即可

    # 表体：第4行起
    body_headers = [
        "电商系统物料编码",
        "辅助自由项编码1",
        "主计量单位",
        "辅计量单位",
        "数量",
        "税率",
        "价税合计",
        "无税金额",
        "税额",
        "含税单价",
        "无税单价",
        "单品折扣",
        "整单折扣",
        "客户订单号",
        "是否赠品",
        "发货仓库编码",
    ]

    for col_idx, name in enumerate(body_headers, start=1):
        ws.cell(row=4, column=col_idx, value=name)

    report_rows = _compute_report_rows(df, aux_map=aux_map, is_return=is_return)
    data_start_row = 5
    for r_idx, row_data in enumerate(report_rows):
        excel_row = data_start_row + r_idx
        for c_idx, v in enumerate(row_data, start=1):
            ws.cell(row=excel_row, column=c_idx, value=v)

    # B2：价税合计列求和（列G）
    if report_rows:
        last_row = data_start_row + len(report_rows) - 1
        ws["B2"] = f"=SUM(G{data_start_row}:G{last_row})"
    else:
        ws["B2"] = 0

    # 简单列宽（不做复杂样式）
    col_widths = {
        1: 16,  # 电商系统物料编码
        2: 16,  # 辅助自由项编码1
        3: 12,
        4: 12,
        5: 10,
        6: 8,
        7: 14,
        8: 14,
        9: 12,
        10: 12,
        11: 12,
        12: 10,
        13: 10,
        14: 18,
        15: 10,
        16: 14,
    }
    for col_idx in range(1, len(body_headers) + 1):
        ws.column_dimensions[get_column_letter(col_idx)].width = col_widths.get(col_idx, 12)

    wb.save(output)
    output.seek(0)
    return output


def _dfs_to_concat(files: List[object], st_label: str) -> Optional[pd.DataFrame]:
    dfs = []
    for f in files or []:
        try:
            df = pd.read_excel(f)
            if df is None or df.empty:
                continue
            df = _normalize_columns(df)
            dfs.append(df)
        except Exception as e:
            st.warning(f"{st_label}读取失败：{e}")
    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def _dfs_to_concat_by_name_keywords(
    files: List[object],
    st_label: str,
    name_keywords: Optional[List[str]],
) -> Optional[pd.DataFrame]:
    """
    按上传文件名宽容过滤读取（用于兜底识别文件）。
    若过滤后为空，会自动回退到“不过滤”。
    """
    if not files:
        return None

    name_keywords = name_keywords or []
    lowered_keywords = [k.lower() for k in name_keywords if k]

    def _match_one(f: object) -> bool:
        if not lowered_keywords:
            return True
        nm = str(getattr(f, "name", "") or "").lower()
        return any(k in nm for k in lowered_keywords)

    dfs: List[pd.DataFrame] = []
    for f in files or []:
        if not _match_one(f):
            continue
        try:
            if hasattr(f, "seek"):
                f.seek(0)
            df = pd.read_excel(f)
            if df is None or df.empty:
                continue
            df = _normalize_columns(df)
            dfs.append(df)
        except Exception as e:
            st.warning(f"{st_label}读取失败：{e}")

    if dfs:
        return pd.concat(dfs, ignore_index=True)

    # 兜底：名字过滤导致空，回退到读取所有
    dfs = []
    for f in files or []:
        try:
            if hasattr(f, "seek"):
                f.seek(0)
            df = pd.read_excel(f)
            if df is None or df.empty:
                continue
            df = _normalize_columns(df)
            dfs.append(df)
        except Exception as e:
            st.warning(f"{st_label}读取失败：{e}")

    if not dfs:
        return None
    return pd.concat(dfs, ignore_index=True)


def _zip_bytes(files: Dict[str, BytesIO]) -> BytesIO:
    zbuf = BytesIO()
    with zipfile.ZipFile(zbuf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, bio in files.items():
            bio.seek(0)
            zf.writestr(name, bio.read())
    zbuf.seek(0)
    return zbuf


def _bucket_report_downloads(
    base_df: pd.DataFrame,
    aux_map: Dict[str, Tuple[str, str]],
    yesterday: date,
    is_return: bool,
) -> Dict[str, BytesIO]:
    """
    base_df: 统一列：店铺/商品编码/实发数量/实际支付金额/线上订单号/订单号
    """
    buckets = {
        "HAY-Tmall": [],
        "RED": [],
        "JD-STUDIO 9": [],
    }
    if base_df is not None and not base_df.empty and "店铺" in base_df.columns:
        for _, r in base_df.iterrows():
            bucket = _shop_bucket(r.get("店铺", ""))
            buckets[bucket].append(r)
    elif base_df is not None and not base_df.empty:
        # 无店铺列时，全部丢到兜底
        buckets["HAY-Tmall"] = [r for _, r in base_df.iterrows()]

    result: Dict[str, BytesIO] = {}
    for bucket_key, items in buckets.items():
        df_bucket = pd.DataFrame(items)
        if df_bucket.empty:
            df_bucket = pd.DataFrame(columns=["店铺", "商品编码", "实发数量", "实际支付金额", "线上订单号", "订单号"])
        file_bytes = _dataframe_to_excel_bytes(df_bucket, yesterday=yesterday, aux_map=aux_map, is_return=is_return)
        result[bucket_key] = file_bytes
    return result


def main() -> None:
    st.set_page_config(page_title="电商财务对账与报表生成", layout="wide")
    st.title("电商财务对账与报表生成")
    st.caption("支持上传多个 .xlsx/.xls 文件，内存处理并直接下载 SO/RT 报表（含 ZIP 打包）。")

    with st.expander("1) 上传文件（允许为空）", expanded=True):
        so1_files = st.file_uploader(
            "销售底表1（聚水潭出库表）",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key="so1_files",
        )
        so2_files = st.file_uploader(
            "销售底表2（WMS发货明细）",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key="so2_files",
        )
        rt3_files = st.file_uploader(
            "退货底表3（聚水潭退货表）",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key="rt3_files",
        )
        rt4_files = st.file_uploader(
            "退货底表4（WMS收货明细）",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key="rt4_files",
        )
        aux_files = st.file_uploader(
            "辅助表（匹配物料单位）",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key="aux_files",
        )
        manual_files = st.file_uploader(
            "手工单表（独立业务）",
            type=["xlsx", "xls"],
            accept_multiple_files=True,
            key="manual_files",
        )

    st.divider()

    colA, colB = st.columns([1, 2])
    with colA:
        run = st.button("生成报表", type="primary")
    with colB:
        st.info("提示：你上传多少就处理多少；缺失的部分会跳过并继续生成可下载结果。")

    if not run:
        st.stop()

    yesterday = date.today() - timedelta(days=1)
    yesterday_prefix = yesterday.strftime("%Y%m%d")

    debug = st.checkbox("显示调试信息（用于定位为什么为0）", value=False)

    with st.spinner("正在读取/处理数据，请稍候..."):
        aux_df = _dfs_to_concat(aux_files, "辅助表")
        aux_map = _build_aux_map(aux_df)

        st.write(f"辅助表：{0 if aux_df is None else len(aux_df)} 行")

        # 宽容文件名识别（如果文件名不规范会自动回退为“不过滤”，避免整批空数据）
        so1_df = (
            _dfs_to_concat_by_name_keywords(
                so1_files,
                "销售底表1",
                ["底表1", "销售出库", "出库表", "聚水潭出库"],
            )
            if so1_files
            else None
        )
        so2_df = (
            _dfs_to_concat_by_name_keywords(
                so2_files,
                "销售底表2",
                ["底表2", "发货明细", "WMS发货", "发货", "WMS"],
            )
            if so2_files
            else None
        )
        rt3_df = (
            _dfs_to_concat_by_name_keywords(
                rt3_files,
                "退货底表3",
                ["底表3", "退货", "售后", "聚水潭退货"],
            )
            if rt3_files
            else None
        )
        rt4_df = (
            _dfs_to_concat_by_name_keywords(
                rt4_files,
                "退货底表4",
                ["底表4", "收货明细", "WMS收货", "收货", "WMS"],
            )
            if rt4_files
            else None
        )
        manual_df = (
            _dfs_to_concat_by_name_keywords(
                manual_files,
                "手工单表",
                ["手工", "手工单", "独立业务"],
            )
            if manual_files
            else None
        )

        if debug:
            st.write("=== Debug：上传文件读取结果 ===")
            st.write(f"aux_df：{None if aux_df is None else aux_df.shape}, sample_cols={[] if aux_df is None else list(aux_df.columns[:20])}")
            st.write(f"so1_df：{None if so1_df is None else so1_df.shape}, sample_cols={[] if so1_df is None else list(so1_df.columns[:20])}")
            st.write(f"so2_df：{None if so2_df is None else so2_df.shape}, sample_cols={[] if so2_df is None else list(so2_df.columns[:20])}")
            st.write(f"rt3_df：{None if rt3_df is None else rt3_df.shape}, sample_cols={[] if rt3_df is None else list(rt3_df.columns[:20])}")
            st.write(f"rt4_df：{None if rt4_df is None else rt4_df.shape}, sample_cols={[] if rt4_df is None else list(rt4_df.columns[:20])}")
            st.write(f"manual_df：{None if manual_df is None else manual_df.shape}, sample_cols={[] if manual_df is None else list(manual_df.columns[:20])}")

        so_base_df = pd.DataFrame()
        if so1_df is not None and so2_df is not None:
            so_base_df = _enrich_so_df(so1_df, so2_df, aux_map=aux_map, debug=debug)
        st.write(f"SO匹配后行数：{len(so_base_df)}")

        rt_main_df = pd.DataFrame()
        rt_nonbao_df = pd.DataFrame()
        if rt3_df is not None and rt4_df is not None:
            rt_main_df, rt_nonbao_df = _enrich_rt_df(rt3_df, rt4_df, aux_map=aux_map, debug=debug)
        st.write(f"RT主退货行数：{len(rt_main_df) if rt_main_df is not None else 0}，RT非保行数：{len(rt_nonbao_df) if rt_nonbao_df is not None else 0}")

        # 手工单：尽量按“同格式处理（SO正 RT负）”
        manual_so_df = pd.DataFrame()
        manual_rt_df = pd.DataFrame()
        if manual_df is not None and not manual_df.empty:
            m = _normalize_columns(manual_df)
            qty_col = _first_existing_col(m, ["实发数量", "数量"])
            amt_col = _first_existing_col(m, ["实发金额", "金额"])
            ship_income_col = _first_existing_col(m, ["运费收入分摊", "运费分摊"])
            sku_name_col = _first_existing_col(m, ["商品简称", "商品名称", "商品描述"])
            sku_code_col = _first_existing_col(m, ["商品编码", "物料编码", "SKU编码", "电商系统物料编码"])
            shop_col = _first_existing_col(m, ["店铺", "店铺名称", "平台店铺"])
            online_order_col = _first_existing_col(m, ["线上订单号", "客户订单号", "线上订单", "订单号线上", "线上订单编号"])
            trx_col = _first_existing_col(m, ["交易类型", "单据类型", "业务类型", "类型", "收发标识"])

            if qty_col and amt_col and sku_code_col:
                _ensure_numeric(m, qty_col)
                _ensure_numeric(m, amt_col)
                if ship_income_col:
                    _ensure_numeric(m, ship_income_col)
                    m["实际支付金额"] = m[amt_col] - m[ship_income_col]
                else:
                    m["实际支付金额"] = m[amt_col]
                    m["运费收入分摊"] = 0

                # 商品排除
                if sku_name_col:
                    m = m.loc[~_contains_keyword(m[sku_name_col], SO_EXCLUDE_KEYWORDS)].copy()

                m["商品编码"] = m[sku_code_col]
                m["实发数量"] = m[qty_col]
                m["线上订单号"] = m[online_order_col].apply(_parse_first_order_id) if online_order_col else ""
                m["店铺"] = m[shop_col] if shop_col else "N/A"
                m["订单号"] = (
                    m.get("出仓单号", None)
                    if "出仓单号" in m.columns
                    else m.get("售后单号", None)
                    if "售后单号" in m.columns
                    else ""
                )

                # 发/退识别：优先看交易类型字段
                if trx_col:
                    trx_series = m[trx_col].astype(str)
                    manual_rt_df = m.loc[trx_series.str.contains("退", na=False)].copy()
                    manual_so_df = m.loc[~trx_series.str.contains("退", na=False)].copy()
                else:
                    # 兜底：用实发数量正负（如果为负则当作退）
                    manual_rt_df = m.loc[m["实发数量"] < 0].copy()
                    manual_so_df = m.loc[m["实发数量"] >= 0].copy()

                # 按需求：发货为正，退货为负
                if not manual_so_df.empty:
                    manual_so_df["实发数量"] = pd.to_numeric(manual_so_df["实发数量"], errors="coerce").fillna(0).abs()
                    manual_so_df["实际支付金额"] = pd.to_numeric(manual_so_df["实际支付金额"], errors="coerce").fillna(0).abs()
                if not manual_rt_df.empty:
                    # 注意：报表生成层（is_return=True）会再做一次 * -1
                    # 因此这里把退货保留为正数的“绝对值”，让报表层统一输出负数
                    manual_rt_df["实发数量"] = pd.to_numeric(manual_rt_df["实发数量"], errors="coerce").fillna(0).abs()
                    manual_rt_df["实际支付金额"] = pd.to_numeric(manual_rt_df["实际支付金额"], errors="coerce").fillna(0).abs()

                manual_so_df = manual_so_df[["店铺", "商品编码", "实发数量", "实际支付金额", "线上订单号", "订单号"]].copy()
                manual_rt_df = manual_rt_df[["店铺", "商品编码", "实发数量", "实际支付金额", "线上订单号", "订单号"]].copy()

        # 组装输出文件
        out_files: Dict[str, BytesIO] = {}

        # SO分流
        so_buckets = _bucket_report_downloads(
            base_df=so_base_df if so_base_df is not None else pd.DataFrame(),
            aux_map=aux_map,
            yesterday=yesterday,
            is_return=False,
        )
        # 兜底输出：仍需so-HAY-Tmall/so-RED/so-JD-STUDIO 9（即使没有数据）
        so_hay_tmall = so_buckets.get("HAY-Tmall")
        so_red = so_buckets.get("RED")
        so_jd = so_buckets.get("JD-STUDIO 9")
        if so_hay_tmall:
            out_files[f"{yesterday_prefix}-so-HAY-Tmall.xlsx"] = so_hay_tmall
        else:
            out_files[f"{yesterday_prefix}-so-HAY-Tmall.xlsx"] = _dataframe_to_excel_bytes(pd.DataFrame(), yesterday, aux_map, is_return=False)
        if so_red:
            out_files[f"{yesterday_prefix}-so-RED.xlsx"] = so_red
        else:
            out_files[f"{yesterday_prefix}-so-RED.xlsx"] = _dataframe_to_excel_bytes(pd.DataFrame(), yesterday, aux_map, is_return=False)
        if so_jd:
            out_files[f"{yesterday_prefix}-so-JD-STUDIO 9.xlsx"] = so_jd
        else:
            out_files[f"{yesterday_prefix}-so-JD-STUDIO 9.xlsx"] = _dataframe_to_excel_bytes(pd.DataFrame(), yesterday, aux_map, is_return=False)

        # so-手工单.xlsx：手工单不再分店铺，按表内SO交易类型汇总
        if manual_so_df is not None and not manual_so_df.empty:
            out_files[f"{yesterday_prefix}-so-手工单.xlsx"] = _dataframe_to_excel_bytes(manual_so_df, yesterday, aux_map, is_return=False)
        else:
            out_files[f"{yesterday_prefix}-so-手工单.xlsx"] = _dataframe_to_excel_bytes(pd.DataFrame(), yesterday, aux_map, is_return=False)

        # RT分流（主退货）
        rt_main_buckets = _bucket_report_downloads(
            base_df=rt_main_df if rt_main_df is not None else pd.DataFrame(),
            aux_map=aux_map,
            yesterday=yesterday,
            is_return=True,
        )
        rt_hay_tmall = rt_main_buckets.get("HAY-Tmall")
        rt_red = rt_main_buckets.get("RED")
        rt_jd = rt_main_buckets.get("JD-STUDIO 9")
        out_files[f"{yesterday_prefix}-rt-HAY-Tmall.xlsx"] = (
            rt_hay_tmall if rt_hay_tmall else _dataframe_to_excel_bytes(pd.DataFrame(), yesterday, aux_map, is_return=True)
        )
        out_files[f"{yesterday_prefix}-rt-RED.xlsx"] = (
            rt_red if rt_red else _dataframe_to_excel_bytes(pd.DataFrame(), yesterday, aux_map, is_return=True)
        )
        out_files[f"{yesterday_prefix}-rt-JD-STUDIO 9.xlsx"] = (
            rt_jd if rt_jd else _dataframe_to_excel_bytes(pd.DataFrame(), yesterday, aux_map, is_return=True)
        )

        # rt-非保.xlsx：仅实发数量==0的退货单
        out_files[f"{yesterday_prefix}-rt-非保.xlsx"] = _dataframe_to_excel_bytes(
            rt_nonbao_df if rt_nonbao_df is not None else pd.DataFrame(), yesterday, aux_map, is_return=True
        )

        # rt-手工单.xlsx
        if manual_rt_df is not None and not manual_rt_df.empty:
            out_files[f"{yesterday_prefix}-rt-手工单.xlsx"] = _dataframe_to_excel_bytes(manual_rt_df, yesterday, aux_map, is_return=True)
        else:
            out_files[f"{yesterday_prefix}-rt-手工单.xlsx"] = _dataframe_to_excel_bytes(pd.DataFrame(), yesterday, aux_map, is_return=True)

        # ZIP打包
        zip_buf = _zip_bytes(out_files)

    st.success("报表已生成，可以开始下载。")
    st.write(
        {
            "so匹配行数": int(len(so_base_df)) if so_base_df is not None else 0,
            "rt主退货行数": int(len(rt_main_df)) if rt_main_df is not None else 0,
            "rt非保退货行数": int(len(rt_nonbao_df)) if rt_nonbao_df is not None else 0,
            "手工单行数": int(len(manual_df)) if manual_df is not None else 0,
        }
    )

    st.subheader("打包下载（ZIP）")
    st.download_button(
        label=f"{yesterday_prefix}-报表ZIP下载",
        data=zip_buf.getvalue(),
        file_name=f"{yesterday_prefix}-报表.zip",
        mime="application/zip",
        key="download_zip",
    )

    st.subheader("单文件下载")
    # 为了界面整洁：两列展示下载按钮
    download_items = list(out_files.items())
    cols = st.columns(2)
    for idx, (fname, bio) in enumerate(download_items):
        col = cols[idx % 2]
        bio.seek(0)
        col.download_button(
            label=fname,
            data=bio.getvalue(),
            file_name=fname,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"download_{fname}",
        )


if __name__ == "__main__":
    main()

