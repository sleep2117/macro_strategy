"""미국 Indeed Job Postings 데이터 통합 모듈."""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import pandas as pd

try:
    from .us_eco_utils import load_economic_data  # type: ignore
except ImportError:
    from us_eco_utils import load_economic_data  # type: ignore


CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), "data", "indeed_jobs_data.csv")

INDEED_JOBS_SERIES: dict[str, str] = {
    # 기본 시리즈
    "total_jobs": "IHLIDXUS",
    "new_jobs": "IHLIDXNEWUS",
    
    # 전체 섹터별 시리즈 (47개)
    "accounting": "IHLIDXUSTPACCO",
    "administrative_assistance": "IHLIDXUSTPADMIASSI",
    "architecture": "IHLIDXUSTPARCH",
    "arts_entertainment": "IHLIDXUSTPAREN",
    "banking_finance": "IHLIDXUSTPBAFI",
    "beauty_wellness": "IHLIDXUSTPBEWE",
    "childcare": "IHLIDXUSTPCHIL",
    "civil_engineering": "IHLIDXUSTPCIVIENGI",
    "cleaning_sanitation": "IHLIDXUSTPCLSA",
    "construction": "IHLIDXUSTPCONS",
    "community_social_service": "IHLIDXUSTPCOSOSE",
    "customer_service": "IHLIDXUSTPCUSTSERV",
    "dental": "IHLIDXUSTPDENT",
    "driving": "IHLIDXUSTPDRIV",
    "education_instruction": "IHLIDXUSTPEDIN",
    "electrical_engineering": "IHLIDXUSTPELECENGI",
    "food_preparation_service": "IHLIDXUSTPFOPRSE",
    "hospitality_tourism": "IHLIDXUSTPHOTO",
    "human_resources": "IHLIDXUSTPHUMARESO",
    "information_design_documentation": "IHLIDXUSTPINDEDO",
    "industrial_engineering": "IHLIDXUSTPINDUENGI",
    "installation_maintenance": "IHLIDXUSTPINMA",
    "insurance": "IHLIDXUSTPINSU",
    "it_operations_helpdesk": "IHLIDXUSTPITOPHE",
    "legal": "IHLIDXUSTPLEGA",
    "logistic_support": "IHLIDXUSTPLOGISUPP",
    "loading_stocking": "IHLIDXUSTPLOST",
    "management": "IHLIDXUSTPMANA",
    "marketing": "IHLIDXUSTPMARK",
    "mathematics": "IHLIDXUSTPMATH",
    "media_communications": "IHLIDXUSTPMECO",
    "medical_information": "IHLIDXUSTPMEDIINFO",
    "medical_technician": "IHLIDXUSTPMEDITECH",
    "nursing": "IHLIDXUSTPNURS",
    "personal_care_home_health": "IHLIDXUSTPPECAHOHE",
    "pharmacy": "IHLIDXUSTPPHAR",
    "physicians_surgeons": "IHLIDXUSTPPHSU",
    "production_manufacturing": "IHLIDXUSTPPRMA",
    "project_management": "IHLIDXUSTPPROJMANA",
    "retail": "IHLIDXUSTPRETA",
    "sales": "IHLIDXUSTPSALE",
    "scientific_research_development": "IHLIDXUSTPSCREDE",
    "security_public_safety": "IHLIDXUSTPSEPUSA",
    "software_development": "IHLIDXUSTPSOFTDEVE",
    "sports": "IHLIDXUSTPSPOR",
    "therapy": "IHLIDXUSTPTHER",
    "veterinary": "IHLIDXUSTPVETE",
}

INDEED_JOBS_KOREAN_NAMES: dict[str, str] = {
    # 기본 시리즈
    "total_jobs": "전체 구직공고",
    "new_jobs": "신규 구직공고",
    
    # 전체 섹터별 한국어 명칭 (47개)
    "accounting": "회계",
    "administrative_assistance": "행정 보조",
    "architecture": "건축",
    "arts_entertainment": "예술/엔터테인먼트",
    "banking_finance": "금융",
    "beauty_wellness": "뷰티/웰니스",
    "childcare": "육아/보육",
    "civil_engineering": "토목공학",
    "cleaning_sanitation": "청소/위생",
    "construction": "건설",
    "community_social_service": "사회복지/커뮤니티 서비스",
    "customer_service": "고객서비스",
    "dental": "치과",
    "driving": "운전/운송",
    "education_instruction": "교육/강의",
    "electrical_engineering": "전기공학",
    "food_preparation_service": "음식 준비/서비스",
    "hospitality_tourism": "숙박/관광",
    "human_resources": "인사",
    "information_design_documentation": "정보 디자인/문서화",
    "industrial_engineering": "산업공학",
    "installation_maintenance": "설치/유지보수",
    "insurance": "보험",
    "it_operations_helpdesk": "IT 운영/헬프데스크",
    "legal": "법무",
    "logistic_support": "물류 지원",
    "loading_stocking": "하역/재고관리",
    "management": "관리직",
    "marketing": "마케팅",
    "mathematics": "수학",
    "media_communications": "미디어/커뮤니케이션",
    "medical_information": "의료 정보",
    "medical_technician": "의료 기술자",
    "nursing": "간호",
    "personal_care_home_health": "개인 돌봄/홈 헬스케어",
    "pharmacy": "약국/약학",
    "physicians_surgeons": "의사/외과의사",
    "production_manufacturing": "생산/제조",
    "project_management": "프로젝트 관리",
    "retail": "소매",
    "sales": "영업",
    "scientific_research_development": "과학연구개발",
    "security_public_safety": "보안/공공안전",
    "software_development": "소프트웨어 개발",
    "sports": "스포츠",
    "therapy": "치료/재활",
    "veterinary": "수의학",
}

INDEED_JOBS_DATA: dict[str, Any] = {
    "raw_data": pd.DataFrame(),
    "mom_data": pd.DataFrame(),
    "mom_change": pd.DataFrame(),
    "yoy_data": pd.DataFrame(),
    "yoy_change": pd.DataFrame(),
    "latest_values": {},
    "load_info": {
        "loaded": False,
        "load_time": None,
        "start_date": None,
        "series_count": 0,
        "data_points": 0,
        "source": None,
    },
}


def _build_latest_values(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    if df is None or df.empty:
        return latest
    for column in df.columns:
        series = pd.to_numeric(df[column], errors="coerce").dropna()
        if series.empty:
            continue
        last_timestamp = series.index[-1]
        latest[column] = {
            "value": float(series.iloc[-1]),
            "date": last_timestamp.strftime("%Y-%m-%d"),
        }
    return latest


def load_indeed_jobs_data(
    start_date: str = "2020-01-01",
    smart_update: bool = True,
    force_reload: bool = False,
) -> dict[str, Any] | None:
    """Load Indeed Job Postings series from FRED."""

    result = load_economic_data(
        INDEED_JOBS_SERIES,
        data_source="FRED",
        csv_file_path=CSV_FILE_PATH,
        start_date=start_date,
        smart_update=smart_update,
        force_reload=force_reload,
    )

    if not result:
        return None

    for key in ("raw_data", "mom_data", "mom_change", "yoy_data", "yoy_change"):
        value = result.get(key)
        INDEED_JOBS_DATA[key] = value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame()

    load_info = result.get("load_info", {}).copy()
    INDEED_JOBS_DATA["load_info"] = load_info

    raw_df = INDEED_JOBS_DATA.get("raw_data", pd.DataFrame())
    latest_values = _build_latest_values(raw_df)
    INDEED_JOBS_DATA["latest_values"] = latest_values
    INDEED_JOBS_DATA["korean_names"] = dict(INDEED_JOBS_KOREAN_NAMES)
    INDEED_JOBS_DATA["load_info"].update(
        {
            "loaded": True,
            "load_time": datetime.now(),
            "start_date": start_date,
            "series_count": raw_df.shape[1] if isinstance(raw_df, pd.DataFrame) else 0,
            "data_points": raw_df.shape[0] if isinstance(raw_df, pd.DataFrame) else 0,
        }
    )

    return INDEED_JOBS_DATA


# 초기 로드는 지연 평가 (대시보드에서 필요 시 호출)