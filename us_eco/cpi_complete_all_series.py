# %%
"""
완전한 CPI 계층 구조 - 325개 모든 BLS 시리즈 포함
제공된 텍스트 기반으로 모든 데이터를 체계적으로 분류
"""

# 325개 모든 CPI 시리즈를 포함하는 완전한 계층 구조
COMPLETE_ALL_CPI_HIERARCHY = {
    # Level 1: 최상위 종합 지표
    'level1': {
        'headline': {
            'bls': 'CUSR0000SA0', 
            'name': 'All items in U.S. city average, all urban consumers, seasonally adjusted',
            'name_kr': '전체 품목 (헤드라인 CPI)'
        },
        'core': {
            'bls': 'CUSR0000SA0L1E', 
            'name': 'All items less food and energy',
            'name_kr': '식품·에너지 제외 (코어 CPI)'
        },
        'headline_non_sa' : {
            'bls': 'CUUR0000SA0',
            'name': 'All items in U.S. city average, all urban consumers, not seasonally adjusted',
            'name_kr': '전체 품목 (헤드라인 CPI, 비계절조정)'
        },
        'core_non_sa': {
            'bls': 'CUUR0000SA0L1E',
            'name': 'All items less food and energy, not seasonally adjusted',
            'name_kr': '식품·에너지 제외 (코어 CPI, 비계절조정)'
        },
        'all_less_food': {
            'bls': 'CUSR0000SA0L1', 
            'name': 'All items less food',
            'name_kr': '식품 제외'
        },
        'all_less_shelter': {
            'bls': 'CUSR0000SA0L2', 
            'name': 'All items less shelter',
            'name_kr': '주거 제외'
        },
        'all_less_energy': {
            'bls': 'CUSR0000SA0LE', 
            'name': 'All items less energy',
            'name_kr': '에너지 제외'
        },
        'energy': {
            'bls': 'CUSR0000SA0E', 
            'name': 'Energy in U.S. city average, all urban consumers, seasonally adjusted',
            'name_kr': '에너지'
        },
        'all_less_medical': {
            'bls': 'CUSR0000SA0L5', 
            'name': 'All items less medical care',
            'name_kr': '의료 제외'
        },
        'all_less_food_shelter': {
            'bls': 'CUSR0000SA0L12', 
            'name': 'All items less food and shelter',
            'name_kr': '식품·주거 제외'
        },
        'all_less_food_shelter_energy': {
            'bls': 'CUSR0000SA0L12E', 
            'name': 'All items less food, shelter, and energy',
            'name_kr': '식품·주거·에너지 제외'
        },
        'all_less_food_shelter_energy_cars': {
            'bls': 'CUSR0000SA0L12E4', 
            'name': 'All items less food, shelter, energy, and used cars and trucks',
            'name_kr': '식품·주거·에너지·중고차 제외'
        }
    },
    
    # Level 2: 주요 카테고리 (상품/서비스 구분)
    'level2': {
        'commodities': {
            'bls': 'CUSR0000SAC', 
            'name': 'Commodities',
            'name_kr': '상품'
        },
        'services': {
            'bls': 'CUSR0000SAS', 
            'name': 'Services',
            'name_kr': '서비스'
        },
        'durables': {
            'bls': 'CUSR0000SAD', 
            'name': 'Durables',
            'name_kr': '내구재',
            'parent': 'commodities'
        },
        'nondurables': {
            'bls': 'CUSR0000SAN', 
            'name': 'Nondurables',
            'name_kr': '비내구재',
            'parent': 'commodities'
        },
        'energy_commodities': {
            'bls': 'CUSR0000SACE', 
            'name': 'Energy commodities',
            'name_kr': '에너지 상품',
            'parent': 'commodities'
        }
    },
    
    # Level 3: 주요 생활비 카테고리
    'level3': {
        'food_beverages': {
            'bls': 'CUSR0000SAF', 
            'name': 'Food and beverages',
            'name_kr': '식품·음료'
        },
        'housing': {
            'bls': 'CUSR0000SAH', 
            'name': 'Housing',
            'name_kr': '주거'
        },
        'apparel': {
            'bls': 'CUSR0000SAA', 
            'name': 'Apparel',
            'name_kr': '의류'
        },
        'transportation': {
            'bls': 'CUSR0000SAT', 
            'name': 'Transportation',
            'name_kr': '교통'
        },
        'medical_care': {
            'bls': 'CUSR0000SAM', 
            'name': 'Medical care',
            'name_kr': '의료'
        },
        'recreation': {
            'bls': 'CUSR0000SAR', 
            'name': 'Recreation',
            'name_kr': '여가'
        },
        'education_communication': {
            'bls': 'CUSR0000SAE', 
            'name': 'Education and communication',
            'name_kr': '교육·통신'
        },
        'other_goods_services': {
            'bls': 'CUSR0000SAG', 
            'name': 'Other goods and services',
            'name_kr': '기타 상품·서비스'
        }
    },
    
    # Level 4: 세부 카테고리 분류
    'level4': {
        # Food 세부 분류
        'food': {
            'bls': 'CUSR0000SAF1', 
            'name': 'Food',
            'name_kr': '식품',
            'parent': 'food_beverages'
        },
        'food_home': {
            'bls': 'CUSR0000SAF11', 
            'name': 'Food at home',
            'name_kr': '가정 식품',
            'parent': 'food'
        },
        'food_away': {
            'bls': 'CUSR0000SEFV', 
            'name': 'Food away from home',
            'name_kr': '외식',
            'parent': 'food'
        },
        'alcoholic_beverages': {
            'bls': 'CUSR0000SAF116', 
            'name': 'Alcoholic beverages',
            'name_kr': '주류',
            'parent': 'food_beverages'
        },
        
        # Housing 세부 분류
        'shelter': {
            'bls': 'CUSR0000SAH1', 
            'name': 'Shelter',
            'name_kr': '주거',
            'parent': 'housing'
        },
        'fuels_utilities': {
            'bls': 'CUSR0000SAH2', 
            'name': 'Fuels and utilities',
            'name_kr': '연료·공공요금',
            'parent': 'housing'
        },
        'household_furnishing': {
            'bls': 'CUSR0000SAH3', 
            'name': 'Household furnishings and operations',
            'name_kr': '가정용품·운영',
            'parent': 'housing'
        },
        'household_energy': {
            'bls': 'CUSR0000SAH21', 
            'name': 'Household energy',
            'name_kr': '가정용 에너지',
            'parent': 'fuels_utilities'
        },
        
        # Transportation 세부 분류
        'private_transportation': {
            'bls': 'CUSR0000SAT1', 
            'name': 'Private transportation',
            'name_kr': '개인교통',
            'parent': 'transportation'
        },
        'new_used_motor_vehicles': {
            'bls': 'CUSR0000SETA', 
            'name': 'New and used motor vehicles',
            'name_kr': '신차·중고차',
            'parent': 'private_transportation'
        },
        'motor_fuel': {
            'bls': 'CUSR0000SETB', 
            'name': 'Motor fuel',
            'name_kr': '자동차 연료',
            'parent': 'private_transportation'
        },
        'motor_vehicle_parts': {
            'bls': 'CUSR0000SETC', 
            'name': 'Motor vehicle parts and equipment',
            'name_kr': '자동차 부품',
            'parent': 'private_transportation'
        },
        'motor_vehicle_maintenance': {
            'bls': 'CUSR0000SETD', 
            'name': 'Motor vehicle maintenance and repair',
            'name_kr': '자동차 정비',
            'parent': 'private_transportation'
        },
        'motor_vehicle_insurance': {
            'bls': 'CUSR0000SETE', 
            'name': 'Motor vehicle insurance',
            'name_kr': '자동차 보험',
            'parent': 'private_transportation'
        },
        'public_transportation': {
            'bls': 'CUSR0000SETG', 
            'name': 'Public transportation',
            'name_kr': '대중교통',
            'parent': 'transportation'
        },
        
        # Medical Care 세부 분류
        'medical_commodities': {
            'bls': 'CUSR0000SAM1', 
            'name': 'Medical care commodities',
            'name_kr': '의료용품',
            'parent': 'medical_care'
        },
        'medical_services': {
            'bls': 'CUSR0000SAM2', 
            'name': 'Medical care services',
            'name_kr': '의료서비스',
            'parent': 'medical_care'
        },
        
        # Apparel 세부 분류
        'mens_boys_apparel': {
            'bls': 'CUSR0000SAA1', 
            'name': "Men's and boys' apparel",
            'name_kr': '남성·남아 의류',
            'parent': 'apparel'
        },
        'womens_girls_apparel': {
            'bls': 'CUSR0000SAA2', 
            'name': "Women's and girls' apparel",
            'name_kr': '여성·여아 의류',
            'parent': 'apparel'
        },
        'apparel_less_footwear': {
            'bls': 'CUSR0000SA311', 
            'name': 'Apparel less footwear',
            'name_kr': '신발 제외 의류',
            'parent': 'apparel'
        },
        
        # Education & Communication 세부 분류
        'education': {
            'bls': 'CUSR0000SAE1', 
            'name': 'Education',
            'name_kr': '교육',
            'parent': 'education_communication'
        },
        'communication': {
            'bls': 'CUSR0000SAE2', 
            'name': 'Communication',
            'name_kr': '통신',
            'parent': 'education_communication'
        },
        'information_processing': {
            'bls': 'CUSR0000SAE21', 
            'name': 'Information and information processing',
            'name_kr': '정보처리',
            'parent': 'communication'
        },
        
        # Recreation 세부 분류
        'recreation_commodities': {
            'bls': 'CUSR0000SARC', 
            'name': 'Recreation commodities',
            'name_kr': '여가 상품',
            'parent': 'recreation'
        },
        'recreation_services': {
            'bls': 'CUSR0000SARS', 
            'name': 'Recreation services',
            'name_kr': '여가 서비스',
            'parent': 'recreation'
        },
        
        # Other Goods and Services 세부 분류
        'personal_care': {
            'bls': 'CUSR0000SAG1', 
            'name': 'Personal care',
            'name_kr': '개인관리',
            'parent': 'other_goods_services'
        },
        'other_goods': {
            'bls': 'CUSR0000SAGC', 
            'name': 'Other goods',
            'name_kr': '기타 상품',
            'parent': 'other_goods_services'
        }
    },
    
    # Level 5: 더 세부적인 분류
    'level5': {
        # Food at Home 세부
        'cereals_bakery': {
            'bls': 'CUSR0000SAF111', 
            'name': 'Cereals and bakery products',
            'name_kr': '곡물·베이커리',
            'parent': 'food_home'
        },
        'meats_poultry_fish_eggs': {
            'bls': 'CUSR0000SAF112', 
            'name': 'Meats, poultry, fish, and eggs',
            'name_kr': '육류·가금류·어류·계란',
            'parent': 'food_home'
        },
        'fruits_vegetables': {
            'bls': 'CUSR0000SAF113', 
            'name': 'Fruits and vegetables',
            'name_kr': '과일·채소',
            'parent': 'food_home'
        },
        'nonalcoholic_beverages': {
            'bls': 'CUSR0000SAF114', 
            'name': 'Nonalcoholic beverages and beverage materials',
            'name_kr': '무알코올 음료',
            'parent': 'food_home'
        },
        'other_food_home': {
            'bls': 'CUSR0000SAF115', 
            'name': 'Other food at home',
            'name_kr': '기타 가정 식품',
            'parent': 'food_home'
        },
        
        # Motor Vehicles 세부
        'new_vehicles': {
            'bls': 'CUSR0000SETA01', 
            'name': 'New vehicles',
            'name_kr': '신차',
            'parent': 'new_used_motor_vehicles'
        },
        'used_cars_trucks': {
            'bls': 'CUSR0000SETA02', 
            'name': 'Used cars and trucks',
            'name_kr': '중고차·트럭',
            'parent': 'new_used_motor_vehicles'
        },
        'leased_cars_trucks': {
            'bls': 'CUSR0000SETA03', 
            'name': 'Leased cars and trucks',
            'name_kr': '리스 차량',
            'parent': 'new_used_motor_vehicles'
        },
        'car_truck_rental': {
            'bls': 'CUSR0000SETA04', 
            'name': 'Car and truck rental',
            'name_kr': '렌터카',
            'parent': 'new_used_motor_vehicles'
        },
        
        # Motor Fuel 세부
        'gasoline': {
            'bls': 'CUSR0000SETB01', 
            'name': 'Gasoline (all types)',
            'name_kr': '휘발유',
            'parent': 'motor_fuel'
        },
        'other_motor_fuels': {
            'bls': 'CUSR0000SETB02', 
            'name': 'Other motor fuels',
            'name_kr': '기타 자동차 연료',
            'parent': 'motor_fuel'
        },
        
        # Shelter 세부
        'rent_primary': {
            'bls': 'CUSR0000SEHA', 
            'name': 'Rent of primary residence',
            'name_kr': '주거 임대료',
            'parent': 'shelter'
        },
        'lodging_away': {
            'bls': 'CUSR0000SEHB', 
            'name': 'Lodging away from home',
            'name_kr': '숙박',
            'parent': 'shelter'
        },
        'owners_equivalent_rent': {
            'bls': 'CUSR0000SEHC', 
            'name': "Owners' equivalent rent of residences",
            'name_kr': '소유자 등가 임대료',
            'parent': 'shelter'
        },
        
        # Energy Services 세부
        'energy_services': {
            'bls': 'CUSR0000SEHF', 
            'name': 'Energy services',
            'name_kr': '에너지 서비스',
            'parent': 'fuels_utilities'
        },
        'electricity': {
            'bls': 'CUSR0000SEHF01', 
            'name': 'Electricity',
            'name_kr': '전기',
            'parent': 'energy_services'
        },
        'utility_gas': {
            'bls': 'CUSR0000SEHF02', 
            'name': 'Utility (piped) gas service',
            'name_kr': '도시가스',
            'parent': 'energy_services'
        },
        'fuel_oil_other': {
            'bls': 'CUSR0000SEHE', 
            'name': 'Fuel oil and other fuels',
            'name_kr': '연료유·기타 연료',
            'parent': 'fuels_utilities'
        },
        'water_sewer_trash': {
            'bls': 'CUSR0000SEHG', 
            'name': 'Water and sewer and trash collection services',
            'name_kr': '상하수도·쓰레기',
            'parent': 'fuels_utilities'
        },
        
        # Medical Services 세부
        'professional_services': {
            'bls': 'CUSR0000SEMC', 
            'name': 'Professional services',
            'name_kr': '전문 의료 서비스',
            'parent': 'medical_services'
        },
        'hospital_services': {
            'bls': 'CUSR0000SEMD', 
            'name': 'Hospital and related services',
            'name_kr': '병원 서비스',
            'parent': 'medical_services'
        },
        'medicinal_drugs': {
            'bls': 'CUSR0000SEMF', 
            'name': 'Medicinal drugs',
            'name_kr': '의약품',
            'parent': 'medical_commodities'
        },
        
        # Public Transportation 세부
        'airline_fares': {
            'bls': 'CUSR0000SETG01', 
            'name': 'Airline fares',
            'name_kr': '항공료',
            'parent': 'public_transportation'
        },
        'other_intercity_transport': {
            'bls': 'CUSR0000SETG02', 
            'name': 'Other intercity transportation',
            'name_kr': '기타 시외교통',
            'parent': 'public_transportation'
        },
        
        # Recreation 세부
        'video_audio': {
            'bls': 'CUSR0000SERA', 
            'name': 'Video and audio',
            'name_kr': '비디오·오디오',
            'parent': 'recreation_commodities'
        },
        'pets_pet_products': {
            'bls': 'CUSR0000SERB', 
            'name': 'Pets, pet products and services',
            'name_kr': '애완동물·용품',
            'parent': 'recreation'
        },
        'sporting_goods': {
            'bls': 'CUSR0000SERC', 
            'name': 'Sporting goods',
            'name_kr': '스포츠용품',
            'parent': 'recreation_commodities'
        },
        'photography': {
            'bls': 'CUSR0000SERD', 
            'name': 'Photography',
            'name_kr': '사진',
            'parent': 'recreation_commodities'
        },
        'other_recreational_goods': {
            'bls': 'CUSR0000SERE', 
            'name': 'Other recreational goods',
            'name_kr': '기타 여가용품',
            'parent': 'recreation_commodities'
        },
        'other_recreation_services': {
            'bls': 'CUSR0000SERF', 
            'name': 'Other recreation services',
            'name_kr': '기타 여가서비스',
            'parent': 'recreation_services'
        },
        'recreational_reading': {
            'bls': 'CUSR0000SERG', 
            'name': 'Recreational reading materials',
            'name_kr': '여가독서자료',
            'parent': 'recreation_commodities'
        },
        
        # Apparel 세부
        'footwear': {
            'bls': 'CUSR0000SEAE', 
            'name': 'Footwear',
            'name_kr': '신발',
            'parent': 'apparel'
        },
        'infants_toddlers_apparel': {
            'bls': 'CUSR0000SEAF', 
            'name': "Infants' and toddlers' apparel",
            'name_kr': '유아·아동 의류',
            'parent': 'apparel'
        },
        'jewelry_watches': {
            'bls': 'CUSR0000SEAG', 
            'name': 'Jewelry and watches',
            'name_kr': '보석·시계',
            'parent': 'apparel'
        },
        
        # Education 세부
        'educational_books': {
            'bls': 'CUSR0000SEEA', 
            'name': 'Educational books and supplies',
            'name_kr': '교육도서·용품',
            'parent': 'education'
        },
        'tuition_childcare': {
            'bls': 'CUSR0000SEEB', 
            'name': 'Tuition, other school fees, and childcare',
            'name_kr': '등록금·보육료',
            'parent': 'education'
        },
        
        # Communication 세부
        'postage_delivery': {
            'bls': 'CUSR0000SEEC', 
            'name': 'Postage and delivery services',
            'name_kr': '우편·배송',
            'parent': 'communication'
        },
        'information_technology': {
            'bls': 'CUSR0000SEEE', 
            'name': 'Information technology, hardware and services',
            'name_kr': '정보기술·하드웨어',
            'parent': 'communication'
        },
        
        # Other Goods and Services 세부
        'tobacco': {
            'bls': 'CUSR0000SEGA', 
            'name': 'Tobacco and smoking products',
            'name_kr': '담배·흡연용품',
            'parent': 'other_goods_services'
        },
        'misc_personal_services': {
            'bls': 'CUSR0000SEGD', 
            'name': 'Miscellaneous personal services',
            'name_kr': '기타 개인서비스',
            'parent': 'other_goods_services'
        },
        'misc_personal_goods': {
            'bls': 'CUSR0000SEGE', 
            'name': 'Miscellaneous personal goods',
            'name_kr': '기타 개인용품',
            'parent': 'other_goods_services'
        },
        
        # Household Furnishings 세부
        'household_furnishings_supplies': {
            'bls': 'CUSR0000SAH31', 
            'name': 'Household furnishings and supplies',
            'name_kr': '가정용품·용구',
            'parent': 'household_furnishing'
        },
        'window_floor_coverings': {
            'bls': 'CUSR0000SEHH', 
            'name': 'Window and floor coverings and other linens',
            'name_kr': '창문·바닥 덮개',
            'parent': 'household_furnishing'
        },
        'furniture_bedding': {
            'bls': 'CUSR0000SEHJ', 
            'name': 'Furniture and bedding',
            'name_kr': '가구·침구',
            'parent': 'household_furnishing'
        },
        'appliances': {
            'bls': 'CUSR0000SEHK', 
            'name': 'Appliances',
            'name_kr': '가전제품',
            'parent': 'household_furnishing'
        },
        'other_household_equipment': {
            'bls': 'CUSR0000SEHL', 
            'name': 'Other household equipment and furnishings',
            'name_kr': '기타 가정용 장비',
            'parent': 'household_furnishing'
        },
        'tools_hardware': {
            'bls': 'CUSR0000SEHM', 
            'name': 'Tools, hardware, outdoor equipment and supplies',
            'name_kr': '공구·하드웨어·야외용품',
            'parent': 'household_furnishing'
        },
        'housekeeping_supplies': {
            'bls': 'CUSR0000SEHN', 
            'name': 'Housekeeping supplies',
            'name_kr': '청소용품',
            'parent': 'household_furnishing'
        }
    },
    
    # Level 6: 가장 세부적인 분류 (주요 상품별)
    'level6': {
        # Cereals and Bakery 세부
        'cereals_cereal_products': {
            'bls': 'CUSR0000SEFA', 
            'name': 'Cereals and cereal products',
            'name_kr': '곡물·시리얼',
            'parent': 'cereals_bakery'
        },
        'bakery_products': {
            'bls': 'CUSR0000SEFB', 
            'name': 'Bakery products',
            'name_kr': '베이커리 제품',
            'parent': 'cereals_bakery'
        },
        
        # Meats 세부
        'meats_poultry_fish': {
            'bls': 'CUSR0000SAF1121', 
            'name': 'Meats, poultry, and fish',
            'name_kr': '육류·가금류·어류',
            'parent': 'meats_poultry_fish_eggs'
        },
        'meats': {
            'bls': 'CUSR0000SAF11211', 
            'name': 'Meats',
            'name_kr': '육류',
            'parent': 'meats_poultry_fish'
        },
        'beef_veal': {
            'bls': 'CUSR0000SEFC', 
            'name': 'Beef and veal',
            'name_kr': '쇠고기·송아지고기',
            'parent': 'meats'
        },
        'pork': {
            'bls': 'CUSR0000SEFD', 
            'name': 'Pork',
            'name_kr': '돼지고기',
            'parent': 'meats'
        },
        'other_meats': {
            'bls': 'CUSR0000SEFE', 
            'name': 'Other meats',
            'name_kr': '기타 육류',
            'parent': 'meats'
        },
        'poultry': {
            'bls': 'CUSR0000SEFF', 
            'name': 'Poultry',
            'name_kr': '가금류',
            'parent': 'meats_poultry_fish'
        },
        'fish_seafood': {
            'bls': 'CUSR0000SEFG', 
            'name': 'Fish and seafood',
            'name_kr': '어류·해산물',
            'parent': 'meats_poultry_fish'
        },
        'eggs': {
            'bls': 'CUSR0000SEFH', 
            'name': 'Eggs',
            'name_kr': '계란',
            'parent': 'meats_poultry_fish_eggs'
        },
        'dairy_products': {
            'bls': 'CUSR0000SEFJ', 
            'name': 'Dairy and related products',
            'name_kr': '유제품',
            'parent': 'food_home'
        },
        
        # Fruits and Vegetables 세부
        'fresh_fruits_vegetables': {
            'bls': 'CUSR0000SAF1131', 
            'name': 'Fresh fruits and vegetables',
            'name_kr': '신선 과일·채소',
            'parent': 'fruits_vegetables'
        },
        'fresh_fruits': {
            'bls': 'CUSR0000SEFK', 
            'name': 'Fresh fruits',
            'name_kr': '신선 과일',
            'parent': 'fresh_fruits_vegetables'
        },
        'fresh_vegetables': {
            'bls': 'CUSR0000SEFL', 
            'name': 'Fresh vegetables',
            'name_kr': '신선 채소',
            'parent': 'fresh_fruits_vegetables'
        },
        'processed_fruits_vegetables': {
            'bls': 'CUSR0000SEFM', 
            'name': 'Processed fruits and vegetables',
            'name_kr': '가공 과일·채소',
            'parent': 'fruits_vegetables'
        },
        
        # Beverages 세부
        'juices_drinks': {
            'bls': 'CUSR0000SEFN', 
            'name': 'Juices and nonalcoholic drinks',
            'name_kr': '주스·무알코올 음료',
            'parent': 'nonalcoholic_beverages'
        },
        'beverage_materials': {
            'bls': 'CUSR0000SEFP', 
            'name': 'Beverage materials including coffee and tea',
            'name_kr': '음료 재료 (커피·차)',
            'parent': 'nonalcoholic_beverages'
        },
        
        # Other Food 세부
        'sugar_sweets': {
            'bls': 'CUSR0000SEFR', 
            'name': 'Sugar and sweets',
            'name_kr': '설탕·과자',
            'parent': 'other_food_home'
        },
        'fats_oils': {
            'bls': 'CUSR0000SEFS', 
            'name': 'Fats and oils',
            'name_kr': '유지류',
            'parent': 'other_food_home'
        },
        'other_foods': {
            'bls': 'CUSR0000SEFT', 
            'name': 'Other foods',
            'name_kr': '기타 식품',
            'parent': 'other_food_home'
        },
        
        # Alcoholic Beverages 세부
        'alcoholic_home': {
            'bls': 'CUSR0000SEFW', 
            'name': 'Alcoholic beverages at home',
            'name_kr': '가정용 주류',
            'parent': 'alcoholic_beverages'
        },
        'alcoholic_away': {
            'bls': 'CUSR0000SEFX', 
            'name': 'Alcoholic beverages away from home',
            'name_kr': '외식 주류',
            'parent': 'alcoholic_beverages'
        },
        
        # Gasoline 세부
        'gasoline_regular': {
            'bls': 'CUSR0000SS47014', 
            'name': 'Gasoline, unleaded regular',
            'name_kr': '일반 무연휘발유',
            'parent': 'gasoline'
        },
        'gasoline_midgrade': {
            'bls': 'CUSR0000SS47015', 
            'name': 'Gasoline, unleaded midgrade',
            'name_kr': '중급 무연휘발유',
            'parent': 'gasoline'
        },
        'gasoline_premium': {
            'bls': 'CUSR0000SS47016', 
            'name': 'Gasoline, unleaded premium',
            'name_kr': '고급 무연휘발유',
            'parent': 'gasoline'
        },
        
        # New Cars 세부
        'new_cars': {
            'bls': 'CUSR0000SS45011', 
            'name': 'New cars',
            'name_kr': '승용차 신차',
            'parent': 'new_vehicles'
        },
        'new_trucks': {
            'bls': 'CUSR0000SS45021', 
            'name': 'New trucks',
            'name_kr': '트럭 신차',
            'parent': 'new_vehicles'
        },
        
        # Physicians Services 세부
        'physicians_services': {
            'bls': 'CUSR0000SEMC01', 
            'name': "Physicians' services",
            'name_kr': '의사 서비스',
            'parent': 'professional_services'
        },
        'dental_services': {
            'bls': 'CUSR0000SEMC02', 
            'name': 'Dental services',
            'name_kr': '치과 서비스',
            'parent': 'professional_services'
        },
        'eyeglasses_eye_care': {
            'bls': 'CUSR0000SEMC03', 
            'name': 'Eyeglasses and eye care',
            'name_kr': '안경·시력관리',
            'parent': 'professional_services'
        },
        'other_medical_professionals': {
            'bls': 'CUSR0000SEMC04', 
            'name': 'Services by other medical professionals',
            'name_kr': '기타 의료전문가 서비스',
            'parent': 'professional_services'
        },
        
        # Hospital Services 세부
        'hospital_services_detail': {
            'bls': 'CUSR0000SEMD01', 
            'name': 'Hospital services',
            'name_kr': '병원 서비스',
            'parent': 'hospital_services'
        },
        'nursing_homes': {
            'bls': 'CUSR0000SEMD02', 
            'name': 'Nursing homes and adult day services',
            'name_kr': '요양원·성인주간보호',
            'parent': 'hospital_services'
        },
        
        # Prescription Drugs 세부
        'prescription_drugs': {
            'bls': 'CUSR0000SEMF01', 
            'name': 'Prescription drugs',
            'name_kr': '처방약',
            'parent': 'medicinal_drugs'
        },
        'nonprescription_drugs': {
            'bls': 'CUSR0000SEMF02', 
            'name': 'Nonprescription drugs',
            'name_kr': '일반의약품',
            'parent': 'medicinal_drugs'
        },
        
        # College Tuition 세부
        'college_tuition': {
            'bls': 'CUSR0000SEEB01', 
            'name': 'College tuition and fees',
            'name_kr': '대학 등록금',
            'parent': 'tuition_childcare'
        },
        'elementary_high_tuition': {
            'bls': 'CUSR0000SEEB02', 
            'name': 'Elementary and high school tuition and fees',
            'name_kr': '초중고 등록금',
            'parent': 'tuition_childcare'
        },
        'day_care_preschool': {
            'bls': 'CUSR0000SEEB03', 
            'name': 'Day care and preschool',
            'name_kr': '보육·유치원',
            'parent': 'tuition_childcare'
        },
        'technical_business_school': {
            'bls': 'CUSR0000SEEB04', 
            'name': 'Technical and business school tuition and fees',
            'name_kr': '기술·비즈니스 스쿨',
            'parent': 'tuition_childcare'
        }
    },
    
    # Level 7: 매우 세부적인 분류 (개별 품목)
    'level7': {
        # Commodities 세부 분류들
        'commodities_less_food': {
            'bls': 'CUSR0000SACL1',
            'name': 'Commodities less food',
            'name_kr': '식품 제외 상품',
            'parent': 'commodities'
        },
        'commodities_less_food_beverages': {
            'bls': 'CUSR0000SACL11',
            'name': 'Commodities less food and beverages',
            'name_kr': '식품·음료 제외 상품',
            'parent': 'commodities'
        },
        'commodities_less_food_energy': {
            'bls': 'CUSR0000SACL1E',
            'name': 'Commodities less food and energy commodities',
            'name_kr': '식품·에너지 제외 상품',
            'parent': 'commodities'
        },
        'commodities_less_food_energy_cars': {
            'bls': 'CUSR0000SACL1E4',
            'name': 'Commodities less food, energy, and used cars and trucks',
            'name_kr': '식품·에너지·중고차 제외 상품',
            'parent': 'commodities'
        },
        
        # 농산물 특별 분류
        'domestically_produced_farm_food': {
            'bls': 'CUSR0000SAN1D',
            'name': 'Domestically produced farm food',
            'name_kr': '국내 농산물',
            'parent': 'nondurables'
        },
        
        # 비내구재 세부
        'nondurables_less_food': {
            'bls': 'CUSR0000SANL1',
            'name': 'Nondurables less food',
            'name_kr': '식품 제외 비내구재',
            'parent': 'nondurables'
        },
        'nondurables_less_food_beverages': {
            'bls': 'CUSR0000SANL11',
            'name': 'Nondurables less food and beverages',
            'name_kr': '식품·음료 제외 비내구재',
            'parent': 'nondurables'
        },
        'nondurables_less_food_beverages_apparel': {
            'bls': 'CUSR0000SANL113',
            'name': 'Nondurables less food, beverages, and apparel',
            'name_kr': '식품·음료·의류 제외 비내구재',
            'parent': 'nondurables'
        },
        'nondurables_less_food_apparel': {
            'bls': 'CUSR0000SANL13',
            'name': 'Nondurables less food and apparel',
            'name_kr': '식품·의류 제외 비내구재',
            'parent': 'nondurables'
        },
        
        # 서비스 세부 분류
        'utilities_public_transportation': {
            'bls': 'CUSR0000SAS24',
            'name': 'Utilities and public transportation',
            'name_kr': '공공서비스·대중교통',
            'parent': 'services'
        },
        'rent_of_shelter': {
            'bls': 'CUSR0000SAS2RS',
            'name': 'Rent of shelter',
            'name_kr': '주거 임대료',
            'parent': 'services'
        },
        'other_services': {
            'bls': 'CUSR0000SAS367',
            'name': 'Other services',
            'name_kr': '기타 서비스',
            'parent': 'services'
        },
        'transportation_services': {
            'bls': 'CUSR0000SAS4',
            'name': 'Transportation services',
            'name_kr': '교통 서비스',
            'parent': 'services'
        },
        'services_less_rent_shelter': {
            'bls': 'CUSR0000SASL2RS',
            'name': 'Services less rent of shelter',
            'name_kr': '주거임대료 제외 서비스',
            'parent': 'services'
        },
        'services_less_medical': {
            'bls': 'CUSR0000SASL5',
            'name': 'Services less medical care services',
            'name_kr': '의료서비스 제외 서비스',
            'parent': 'services'
        },
        'services_less_energy': {
            'bls': 'CUSR0000SASLE',
            'name': 'Services less energy services',
            'name_kr': '에너지 서비스 제외 서비스',
            'parent': 'services'
        },
        
        # Transportation 세부
        'transportation_commodities_less_motor_fuel': {
            'bls': 'CUSR0000SATCLTB',
            'name': 'Transportation commodities less motor fuel',
            'name_kr': '자동차 연료 제외 교통상품',
            'parent': 'transportation'
        },
        
        # Education & Communication 세부
        'education_communication_commodities': {
            'bls': 'CUSR0000SAEC',
            'name': 'Education and communication commodities',
            'name_kr': '교육·통신 상품',
            'parent': 'education_communication'
        },
        'education_communication_services': {
            'bls': 'CUSR0000SAES',
            'name': 'Education and communication services',
            'name_kr': '교육·통신 서비스',
            'parent': 'education_communication'
        },
        'information_technology_commodities': {
            'bls': 'CUSR0000SEEEC',
            'name': 'Information technology commodities',
            'name_kr': '정보기술 상품',
            'parent': 'education_communication'
        },
        
        # 음식 세부 품목들
        'full_service_meals': {
            'bls': 'CUSR0000SEFV01',
            'name': 'Full service meals and snacks',
            'name_kr': '정식 식사·간식',
            'parent': 'food_away'
        },
        'food_employee_sites_schools': {
            'bls': 'CUSR0000SEFV03',
            'name': 'Food at employee sites and schools',
            'name_kr': '직장·학교 식당',
            'parent': 'food_away'
        },
        'other_food_away': {
            'bls': 'CUSR0000SEFV05',
            'name': 'Other food away from home',
            'name_kr': '기타 외식',
            'parent': 'food_away'
        },
        'food_elementary_secondary_schools': {
            'bls': 'CUSR0000SSFV031A',
            'name': 'Food at elementary and secondary schools',
            'name_kr': '초중고 급식',
            'parent': 'food_away'
        }
    },
    
    # Level 8: 가장 세부적인 개별 품목들
    'level8': {
        # Men's Apparel 세부
        'mens_apparel': {
            'bls': 'CUSR0000SEAA',
            'name': "Men's apparel",
            'name_kr': '남성 의류',
            'parent': 'mens_boys_apparel'
        },
        'mens_suits_outerwear': {
            'bls': 'CUSR0000SEAA01',
            'name': "Men's suits, sport coats, and outerwear",
            'name_kr': '남성 정장·외투',
            'parent': 'mens_apparel'
        },
        'mens_underwear_accessories': {
            'bls': 'CUSR0000SEAA02',
            'name': "Men's underwear, nightwear, swimwear, and accessories",
            'name_kr': '남성 속옷·액세서리',
            'parent': 'mens_apparel'
        },
        'mens_shirts_sweaters': {
            'bls': 'CUSR0000SEAA03',
            'name': "Men's shirts and sweaters",
            'name_kr': '남성 셔츠·스웨터',
            'parent': 'mens_apparel'
        },
        'mens_pants_shorts': {
            'bls': 'CUSR0000SEAA04',
            'name': "Men's pants and shorts",
            'name_kr': '남성 바지·반바지',
            'parent': 'mens_apparel'
        },
        'boys_apparel': {
            'bls': 'CUSR0000SEAB',
            'name': "Boys' apparel",
            'name_kr': '남아 의류',
            'parent': 'mens_boys_apparel'
        },
        
        # Women's Apparel 세부
        'womens_apparel': {
            'bls': 'CUSR0000SEAC',
            'name': "Women's apparel",
            'name_kr': '여성 의류',
            'parent': 'womens_girls_apparel'
        },
        'womens_outerwear': {
            'bls': 'CUSR0000SEAC01',
            'name': "Women's outerwear",
            'name_kr': '여성 외투',
            'parent': 'womens_apparel'
        },
        'womens_dresses': {
            'bls': 'CUSR0000SEAC02',
            'name': "Women's dresses",
            'name_kr': '여성 드레스',
            'parent': 'womens_apparel'
        },
        'womens_suits_separates': {
            'bls': 'CUSR0000SEAC03',
            'name': "Women's suits and separates",
            'name_kr': '여성 정장·세퍼레이트',
            'parent': 'womens_apparel'
        },
        'womens_underwear_accessories': {
            'bls': 'CUSR0000SEAC04',
            'name': "Women's underwear, nightwear, swimwear, and accessories",
            'name_kr': '여성 속옷·액세서리',
            'parent': 'womens_apparel'
        },
        'girls_apparel': {
            'bls': 'CUSR0000SEAD',
            'name': "Girls' apparel",
            'name_kr': '여아 의류',
            'parent': 'womens_girls_apparel'
        },
        
        # Footwear 세부
        'mens_footwear': {
            'bls': 'CUSR0000SEAE01',
            'name': "Men's footwear",
            'name_kr': '남성 신발',
            'parent': 'footwear'
        },
        'boys_girls_footwear': {
            'bls': 'CUSR0000SEAE02',
            'name': "Boys' and girls' footwear",
            'name_kr': '아동 신발',
            'parent': 'footwear'
        },
        'womens_footwear': {
            'bls': 'CUSR0000SEAE03',
            'name': "Women's footwear",
            'name_kr': '여성 신발',
            'parent': 'footwear'
        },
        
        # Jewelry 세부
        'watches': {
            'bls': 'CUSR0000SEAG01',
            'name': 'Watches',
            'name_kr': '시계',
            'parent': 'jewelry_watches'
        },
        'jewelry': {
            'bls': 'CUSR0000SEAG02',
            'name': 'Jewelry',
            'name_kr': '보석',
            'parent': 'jewelry_watches'
        },
        
        # 곡물 및 베이커리 세부
        'flour_flour_mixes': {
            'bls': 'CUSR0000SEFA01',
            'name': 'Flour and prepared flour mixes',
            'name_kr': '밀가루·믹스',
            'parent': 'cereals_cereal_products'
        },
        'breakfast_cereal': {
            'bls': 'CUSR0000SEFA02',
            'name': 'Breakfast cereal',
            'name_kr': '시리얼',
            'parent': 'cereals_cereal_products'
        },
        'rice_pasta_cornmeal': {
            'bls': 'CUSR0000SEFA03',
            'name': 'Rice, pasta, cornmeal',
            'name_kr': '쌀·파스타·옥수수가루',
            'parent': 'cereals_cereal_products'
        },
        'bread': {
            'bls': 'CUSR0000SEFB01',
            'name': 'Bread',
            'name_kr': '빵',
            'parent': 'bakery_products'
        },
        'fresh_biscuits_rolls_muffins': {
            'bls': 'CUSR0000SEFB02',
            'name': 'Fresh biscuits, rolls, muffins',
            'name_kr': '신선 비스킷·롤·머핀',
            'parent': 'bakery_products'
        },
        'cakes_cupcakes_cookies': {
            'bls': 'CUSR0000SEFB03',
            'name': 'Cakes, cupcakes, and cookies',
            'name_kr': '케이크·컵케이크·쿠키',
            'parent': 'bakery_products'
        },
        'other_bakery_products': {
            'bls': 'CUSR0000SEFB04',
            'name': 'Other bakery products',
            'name_kr': '기타 베이커리',
            'parent': 'bakery_products'
        },
        'cookies': {
            'bls': 'CUSR0000SS02042',
            'name': 'Cookies',
            'name_kr': '쿠키',
            'parent': 'bakery_products'
        },
        'crackers_bread_cracker_products': {
            'bls': 'CUSR0000SS0206A',
            'name': 'Crackers, bread, and cracker products',
            'name_kr': '크래커·빵·크래커 제품',
            'parent': 'bakery_products'
        },
        'frozen_refrigerated_bakery': {
            'bls': 'CUSR0000SS0206B',
            'name': 'Frozen and refrigerated bakery products, pies, tarts, turnovers',
            'name_kr': '냉동·냉장 베이커리',
            'parent': 'bakery_products'
        },
        
        # 육류 세부 품목들
        'uncooked_ground_beef': {
            'bls': 'CUSR0000SEFC01',
            'name': 'Uncooked ground beef',
            'name_kr': '생 다진 쇠고기',
            'parent': 'beef_veal'
        },
        'uncooked_beef_roasts': {
            'bls': 'CUSR0000SEFC02',
            'name': 'Uncooked beef roasts',
            'name_kr': '생 쇠고기 로스트',
            'parent': 'beef_veal'
        },
        'uncooked_beef_steaks': {
            'bls': 'CUSR0000SEFC03',
            'name': 'Uncooked beef steaks',
            'name_kr': '생 쇠고기 스테이크',
            'parent': 'beef_veal'
        },
        'bacon_breakfast_sausage': {
            'bls': 'CUSR0000SEFD01',
            'name': 'Bacon, breakfast sausage, and related products',
            'name_kr': '베이컨·소시지',
            'parent': 'pork'
        },
        'ham': {
            'bls': 'CUSR0000SEFD02',
            'name': 'Ham',
            'name_kr': '햄',
            'parent': 'pork'
        },
        'pork_chops': {
            'bls': 'CUSR0000SEFD03',
            'name': 'Pork chops',
            'name_kr': '돼지 갈비',
            'parent': 'pork'
        },
        'other_pork': {
            'bls': 'CUSR0000SEFD04',
            'name': 'Other pork including roasts, steaks, and ribs',
            'name_kr': '기타 돼지고기',
            'parent': 'pork'
        },
        'chicken': {
            'bls': 'CUSR0000SEFF01',
            'name': 'Chicken',
            'name_kr': '닭고기',
            'parent': 'poultry'
        },
        'other_poultry': {
            'bls': 'CUSR0000SEFF02',
            'name': 'Other uncooked poultry including turkey',
            'name_kr': '기타 가금류',
            'parent': 'poultry'
        },
        'fresh_fish_seafood': {
            'bls': 'CUSR0000SEFG01',
            'name': 'Fresh fish and seafood',
            'name_kr': '신선 어류·해산물',
            'parent': 'fish_seafood'
        },
        'processed_fish_seafood': {
            'bls': 'CUSR0000SEFG02',
            'name': 'Processed fish and seafood',
            'name_kr': '가공 어류·해산물',
            'parent': 'fish_seafood'
        },
        
        # 개별 육류 상품들 (SS 시리즈)
        'bacon_related_products': {
            'bls': 'CUSR0000SS04011',
            'name': 'Bacon and related products',
            'name_kr': '베이컨 관련 제품',
            'parent': 'pork'
        },
        'breakfast_sausage_related': {
            'bls': 'CUSR0000SS04012',
            'name': 'Breakfast sausage and related products',
            'name_kr': '조식 소시지',
            'parent': 'pork'
        },
        'ham_excluding_canned': {
            'bls': 'CUSR0000SS04031',
            'name': 'Ham, excluding canned',
            'name_kr': '햄 (통조림 제외)',
            'parent': 'pork'
        },
        'frankfurters': {
            'bls': 'CUSR0000SS05011',
            'name': 'Frankfurters',
            'name_kr': '프랑크푸르트',
            'parent': 'other_meats'
        },
        'lunchmeats': {
            'bls': 'CUSR0000SS0501A',
            'name': 'Lunchmeats',
            'name_kr': '점심용 육류',
            'parent': 'other_meats'
        },
        'fresh_whole_chicken': {
            'bls': 'CUSR0000SS06011',
            'name': 'Fresh whole chicken',
            'name_kr': '생 통닭',
            'parent': 'chicken'
        },
        'fresh_frozen_chicken_parts': {
            'bls': 'CUSR0000SS06021',
            'name': 'Fresh and frozen chicken parts',
            'name_kr': '생 및 냉동 닭고기 부위',
            'parent': 'chicken'
        },
        'shelf_stable_fish': {
            'bls': 'CUSR0000SS07011',
            'name': 'Shelf stable fish and seafood',
            'name_kr': '상온보관 어류·해산물',
            'parent': 'fish_seafood'
        },
        'frozen_fish_seafood': {
            'bls': 'CUSR0000SS07021',
            'name': 'Frozen fish and seafood',
            'name_kr': '냉동 어류·해산물',
            'parent': 'fish_seafood'
        },
        
        # 유제품 세부
        'milk': {
            'bls': 'CUSR0000SEFJ01',
            'name': 'Milk',
            'name_kr': '우유',
            'parent': 'dairy_products'
        },
        'cheese_related': {
            'bls': 'CUSR0000SEFJ02',
            'name': 'Cheese and related products',
            'name_kr': '치즈 관련 제품',
            'parent': 'dairy_products'
        },
        'ice_cream_related': {
            'bls': 'CUSR0000SEFJ03',
            'name': 'Ice cream and related products',
            'name_kr': '아이스크림 관련 제품',
            'parent': 'dairy_products'
        },
        'other_dairy_related': {
            'bls': 'CUSR0000SEFJ04',
            'name': 'Other dairy and related products',
            'name_kr': '기타 유제품',
            'parent': 'dairy_products'
        },
        'fresh_whole_milk': {
            'bls': 'CUSR0000SS09011',
            'name': 'Fresh whole milk',
            'name_kr': '신선 전유',
            'parent': 'milk'
        },
        'fresh_milk_other_than_whole': {
            'bls': 'CUSR0000SS09021',
            'name': 'Fresh milk other than whole',
            'name_kr': '저지방 우유',
            'parent': 'milk'
        },
        'butter': {
            'bls': 'CUSR0000SS10011',
            'name': 'Butter',
            'name_kr': '버터',
            'parent': 'dairy_products'
        },
        
        # 과일 세부
        'apples': {
            'bls': 'CUSR0000SEFK01',
            'name': 'Apples',
            'name_kr': '사과',
            'parent': 'fresh_fruits'
        },
        'bananas': {
            'bls': 'CUSR0000SEFK02',
            'name': 'Bananas',
            'name_kr': '바나나',
            'parent': 'fresh_fruits'
        },
        'citrus_fruits': {
            'bls': 'CUSR0000SEFK03',
            'name': 'Citrus fruits',
            'name_kr': '감귤류',
            'parent': 'fresh_fruits'
        },
        'other_fresh_fruits': {
            'bls': 'CUSR0000SEFK04',
            'name': 'Other fresh fruits',
            'name_kr': '기타 신선 과일',
            'parent': 'fresh_fruits'
        },
        'oranges_tangerines': {
            'bls': 'CUSR0000SS11031',
            'name': 'Oranges, including tangerines',
            'name_kr': '오렌지·귤',
            'parent': 'citrus_fruits'
        },
        
        # 채소 세부
        'potatoes': {
            'bls': 'CUSR0000SEFL01',
            'name': 'Potatoes',
            'name_kr': '감자',
            'parent': 'fresh_vegetables'
        },
        'lettuce': {
            'bls': 'CUSR0000SEFL02',
            'name': 'Lettuce',
            'name_kr': '양상추',
            'parent': 'fresh_vegetables'
        },
        'tomatoes': {
            'bls': 'CUSR0000SEFL03',
            'name': 'Tomatoes',
            'name_kr': '토마토',
            'parent': 'fresh_vegetables'
        },
        'other_fresh_vegetables': {
            'bls': 'CUSR0000SEFL04',
            'name': 'Other fresh vegetables',
            'name_kr': '기타 신선 채소',
            'parent': 'fresh_vegetables'
        },
        
        # 가공 과일·채소
        'canned_fruits_vegetables': {
            'bls': 'CUSR0000SEFM01',
            'name': 'Canned fruits and vegetables',
            'name_kr': '통조림 과일·채소',
            'parent': 'processed_fruits_vegetables'
        },
        'frozen_fruits_vegetables': {
            'bls': 'CUSR0000SEFM02',
            'name': 'Frozen fruits and vegetables',
            'name_kr': '냉동 과일·채소',
            'parent': 'processed_fruits_vegetables'
        },
        'other_processed_fruits_vegetables': {
            'bls': 'CUSR0000SEFM03',
            'name': 'Other processed fruits and vegetables including dried',
            'name_kr': '기타 가공 과일·채소',
            'parent': 'processed_fruits_vegetables'
        },
        'canned_fruits': {
            'bls': 'CUSR0000SS13031',
            'name': 'Canned fruits',
            'name_kr': '통조림 과일',
            'parent': 'processed_fruits_vegetables'
        },
        'frozen_vegetables': {
            'bls': 'CUSR0000SS14011',
            'name': 'Frozen vegetables',
            'name_kr': '냉동 채소',
            'parent': 'processed_fruits_vegetables'
        },
        'canned_vegetables': {
            'bls': 'CUSR0000SS14021',
            'name': 'Canned vegetables',
            'name_kr': '통조림 채소',
            'parent': 'processed_fruits_vegetables'
        },
        
        # 음료 세부
        'carbonated_drinks': {
            'bls': 'CUSR0000SEFN01',
            'name': 'Carbonated drinks',
            'name_kr': '탄산음료',
            'parent': 'juices_drinks'
        },
        'nonfrozen_noncarbonated_juices': {
            'bls': 'CUSR0000SEFN03',
            'name': 'Nonfrozen noncarbonated juices and drinks',
            'name_kr': '생과일 주스·음료',
            'parent': 'juices_drinks'
        },
        'coffee': {
            'bls': 'CUSR0000SEFP01',
            'name': 'Coffee',
            'name_kr': '커피',
            'parent': 'beverage_materials'
        },
        'other_beverage_materials': {
            'bls': 'CUSR0000SEFP02',
            'name': 'Other beverage materials including tea',
            'name_kr': '차 등 기타 음료재료',
            'parent': 'beverage_materials'
        },
        'roasted_coffee': {
            'bls': 'CUSR0000SS17031',
            'name': 'Roasted coffee',
            'name_kr': '로스팅 커피',
            'parent': 'coffee'
        },
        
        # 기타 식품 세부
        'sugar_sugar_substitutes': {
            'bls': 'CUSR0000SEFR01',
            'name': 'Sugar and sugar substitutes',
            'name_kr': '설탕·감미료',
            'parent': 'sugar_sweets'
        },
        'candy_chewing_gum': {
            'bls': 'CUSR0000SEFR02',
            'name': 'Candy and chewing gum',
            'name_kr': '사탕·껌',
            'parent': 'sugar_sweets'
        },
        'other_sweets': {
            'bls': 'CUSR0000SEFR03',
            'name': 'Other sweets',
            'name_kr': '기타 과자',
            'parent': 'sugar_sweets'
        },
        'butter_margarine': {
            'bls': 'CUSR0000SEFS01',
            'name': 'Butter and margarine',
            'name_kr': '버터·마가린',
            'parent': 'fats_oils'
        },
        'salad_dressing': {
            'bls': 'CUSR0000SEFS02',
            'name': 'Salad dressing',
            'name_kr': '샐러드 드레싱',
            'parent': 'fats_oils'
        },
        'other_fats_oils': {
            'bls': 'CUSR0000SEFS03',
            'name': 'Other fats and oils including peanut butter',
            'name_kr': '기타 유지류',
            'parent': 'fats_oils'
        },
        'margarine': {
            'bls': 'CUSR0000SS16011',
            'name': 'Margarine',
            'name_kr': '마가린',
            'parent': 'fats_oils'
        },
        'soups': {
            'bls': 'CUSR0000SEFT01',
            'name': 'Soups',
            'name_kr': '수프',
            'parent': 'other_foods'
        },
        'frozen_prepared_foods': {
            'bls': 'CUSR0000SEFT02',
            'name': 'Frozen and freeze dried prepared foods',
            'name_kr': '냉동 조리식품',
            'parent': 'other_foods'
        },
        'snacks': {
            'bls': 'CUSR0000SEFT03',
            'name': 'Snacks',
            'name_kr': '스낵',
            'parent': 'other_foods'
        },
        'spices_seasonings_condiments': {
            'bls': 'CUSR0000SEFT04',
            'name': 'Spices, seasonings, condiments, sauces',
            'name_kr': '향신료·조미료',
            'parent': 'other_foods'
        },
        'other_misc_foods': {
            'bls': 'CUSR0000SEFT06',
            'name': 'Other miscellaneous foods',
            'name_kr': '기타 잡식품',
            'parent': 'other_foods'
        },
        'salt_seasonings_spices': {
            'bls': 'CUSR0000SS18041',
            'name': 'Salt and other seasonings and spices',
            'name_kr': '소금·향신료',
            'parent': 'other_foods'
        },
        'olives_pickles_relishes': {
            'bls': 'CUSR0000SS18042',
            'name': 'Olives, pickles, relishes',
            'name_kr': '올리브·피클',
            'parent': 'other_foods'
        },
        'sauces_gravies': {
            'bls': 'CUSR0000SS18043',
            'name': 'Sauces and gravies',
            'name_kr': '소스·그레이비',
            'parent': 'other_foods'
        },
        'other_condiments': {
            'bls': 'CUSR0000SS1804B',
            'name': 'Other condiments',
            'name_kr': '기타 조미료',
            'parent': 'other_foods'
        },
        'prepared_salads': {
            'bls': 'CUSR0000SS18064',
            'name': 'Prepared salads',
            'name_kr': '조제 샐러드',
            'parent': 'other_foods'
        },
        
        # 주류 세부
        'beer_ale_malt_beverages': {
            'bls': 'CUSR0000SEFW01',
            'name': 'Beer, ale, and other malt beverages at home',
            'name_kr': '맥주·에일 (가정용)',
            'parent': 'alcoholic_home'
        },
        'distilled_spirits_home': {
            'bls': 'CUSR0000SEFW02',
            'name': 'Distilled spirits at home',
            'name_kr': '증류주 (가정용)',
            'parent': 'alcoholic_home'
        },
        'wine_home': {
            'bls': 'CUSR0000SEFW03',
            'name': 'Wine at home',
            'name_kr': '와인 (가정용)',
            'parent': 'alcoholic_home'
        },
        'whiskey_home': {
            'bls': 'CUSR0000SS20021',
            'name': 'Whiskey at home',
            'name_kr': '위스키 (가정용)',
            'parent': 'alcoholic_home'
        },
        'distilled_spirits_excluding_whiskey': {
            'bls': 'CUSR0000SS20022',
            'name': 'Distilled spirits, excluding whiskey, at home',
            'name_kr': '위스키 제외 증류주 (가정용)',
            'parent': 'alcoholic_home'
        },
        'distilled_spirits_away': {
            'bls': 'CUSR0000SS20053',
            'name': 'Distilled spirits away from home',
            'name_kr': '증류주 (외식)',
            'parent': 'alcoholic_away'
        },
        
        # 주거 세부
        'housing_school_excluding_board': {
            'bls': 'CUSR0000SEHB01',
            'name': 'Housing at school, excluding board',
            'name_kr': '학교 기숙사',
            'parent': 'lodging_away'
        },
        'other_lodging_hotels_motels': {
            'bls': 'CUSR0000SEHB02',
            'name': 'Other lodging away from home including hotels and motels',
            'name_kr': '호텔·모텔 등 숙박',
            'parent': 'lodging_away'
        },
        'owners_equivalent_rent_primary': {
            'bls': 'CUSR0000SEHC01',
            'name': "Owners' equivalent rent of primary residence",
            'name_kr': '주거 소유자 등가임대료',
            'parent': 'owners_equivalent_rent'
        },
        'fuel_oil': {
            'bls': 'CUSR0000SEHE01',
            'name': 'Fuel oil',
            'name_kr': '연료유',
            'parent': 'fuel_oil_other'
        },
        'propane_kerosene_firewood': {
            'bls': 'CUSR0000SEHE02',
            'name': 'Propane, kerosene, and firewood',
            'name_kr': '프로판·등유·장작',
            'parent': 'fuel_oil_other'
        },
        'water_sewerage_maintenance': {
            'bls': 'CUSR0000SEHG01',
            'name': 'Water and sewerage maintenance',
            'name_kr': '상하수도',
            'parent': 'water_sewer_trash'
        },
        'garbage_trash_collection': {
            'bls': 'CUSR0000SEHG02',
            'name': 'Garbage and trash collection',
            'name_kr': '쓰레기 수거',
            'parent': 'water_sewer_trash'
        },
        'window_coverings': {
            'bls': 'CUSR0000SEHH02',
            'name': 'Window coverings',
            'name_kr': '창문 덮개',
            'parent': 'window_floor_coverings'
        },
        'other_linens': {
            'bls': 'CUSR0000SEHH03',
            'name': 'Other linens',
            'name_kr': '기타 리넨',
            'parent': 'window_floor_coverings'
        },
        'other_furniture': {
            'bls': 'CUSR0000SEHJ03',
            'name': 'Other furniture',
            'name_kr': '기타 가구',
            'parent': 'furniture_bedding'
        },
        'major_appliances': {
            'bls': 'CUSR0000SEHK01',
            'name': 'Major appliances',
            'name_kr': '대형 가전',
            'parent': 'appliances'
        },
        'other_appliances': {
            'bls': 'CUSR0000SEHK02',
            'name': 'Other appliances',
            'name_kr': '기타 가전',
            'parent': 'appliances'
        },
        'indoor_plants_flowers': {
            'bls': 'CUSR0000SEHL02',
            'name': 'Indoor plants and flowers',
            'name_kr': '실내 식물·꽃',
            'parent': 'other_household_equipment'
        },
        'nonelectric_cookware_tableware': {
            'bls': 'CUSR0000SEHL04',
            'name': 'Nonelectric cookware and tableware',
            'name_kr': '비전기 주방용품',
            'parent': 'other_household_equipment'
        },
        'tools_hardware_supplies': {
            'bls': 'CUSR0000SEHM01',
            'name': 'Tools, hardware and supplies',
            'name_kr': '공구·하드웨어',
            'parent': 'tools_hardware'
        },
        'outdoor_equipment_supplies': {
            'bls': 'CUSR0000SEHM02',
            'name': 'Outdoor equipment and supplies',
            'name_kr': '야외 장비·용품',
            'parent': 'tools_hardware'
        },
        'household_cleaning_products': {
            'bls': 'CUSR0000SEHN01',
            'name': 'Household cleaning products',
            'name_kr': '가정용 청소용품',
            'parent': 'housekeeping_supplies'
        },
        'misc_household_products': {
            'bls': 'CUSR0000SEHN03',
            'name': 'Miscellaneous household products',
            'name_kr': '기타 가정용품',
            'parent': 'housekeeping_supplies'
        },
        'moving_storage_freight': {
            'bls': 'CUSR0000SEHP03',
            'name': 'Moving, storage, freight expense',
            'name_kr': '이사·보관·운송비',
            'parent': 'household_furnishing'
        },
        'laundry_equipment': {
            'bls': 'CUSR0000SS30021',
            'name': 'Laundry equipment',
            'name_kr': '세탁장비',
            'parent': 'major_appliances'
        },
        'stationery_gift_wrap': {
            'bls': 'CUSR0000SS33032',
            'name': 'Stationery, stationery supplies, gift wrap',
            'name_kr': '문구·포장지',
            'parent': 'other_household_equipment'
        },
        
        # 자동차 세부
        'new_cars_trucks': {
            'bls': 'CUSR0000SS4501A',
            'name': 'New cars and trucks',
            'name_kr': '신차 (승용차+트럭)',
            'parent': 'new_vehicles'
        },
        'new_motorcycles': {
            'bls': 'CUSR0000SS45031',
            'name': 'New motorcycles',
            'name_kr': '신형 모터사이클',
            'parent': 'new_vehicles'
        },
        'tires': {
            'bls': 'CUSR0000SETC01',
            'name': 'Tires',
            'name_kr': '타이어',
            'parent': 'motor_vehicle_parts'
        },
        'motor_vehicle_repair': {
            'bls': 'CUSR0000SETD03',
            'name': 'Motor vehicle repair',
            'name_kr': '자동차 수리',
            'parent': 'motor_vehicle_maintenance'
        },
        'parking_other_fees': {
            'bls': 'CUSR0000SETF03',
            'name': 'Parking and other fees',
            'name_kr': '주차·기타 요금',
            'parent': 'transportation'
        },
        'parking_fees_tolls': {
            'bls': 'CUSR0000SS52051',
            'name': 'Parking fees and tolls',
            'name_kr': '주차비·통행료',
            'parent': 'transportation'
        },
        'intercity_train_fare': {
            'bls': 'CUSR0000SS53022',
            'name': 'Intercity train fare',
            'name_kr': '시외 기차요금',
            'parent': 'other_intercity_transport'
        },
        'ship_fare': {
            'bls': 'CUSR0000SS53023',
            'name': 'Ship fare',
            'name_kr': '선박 요금',
            'parent': 'other_intercity_transport'
        },
        
        # 의료 세부
        'inpatient_hospital_services': {
            'bls': 'CUSR0000SS5702',
            'name': 'Inpatient hospital services',
            'name_kr': '입원 서비스',
            'parent': 'hospital_services'
        },
        'outpatient_hospital_services': {
            'bls': 'CUSR0000SS5703',
            'name': 'Outpatient hospital services',
            'name_kr': '외래 서비스',
            'parent': 'hospital_services'
        },
        
        # 교육 세부
        'postage': {
            'bls': 'CUSR0000SEEC01',
            'name': 'Postage',
            'name_kr': '우편요금',
            'parent': 'postage_delivery'
        },
        'delivery_services': {
            'bls': 'CUSR0000SEEC02',
            'name': 'Delivery services',
            'name_kr': '배송 서비스',
            'parent': 'postage_delivery'
        },
        'computers_peripherals_smart_home': {
            'bls': 'CUSR0000SEEE01',
            'name': 'Computers, peripherals, and smart home assistants',
            'name_kr': '컴퓨터·주변기기·스마트홈',
            'parent': 'information_technology'
        },
        'internet_services': {
            'bls': 'CUSR0000SEEE03',
            'name': 'Internet services and electronic information providers',
            'name_kr': '인터넷 서비스',
            'parent': 'information_technology'
        },
        'telephone_hardware_calculators': {
            'bls': 'CUSR0000SEEE04',
            'name': 'Telephone hardware, calculators, and other consumer information items',
            'name_kr': '전화기·계산기 등',
            'parent': 'information_technology'
        },
        
        # 여가 세부
        'televisions': {
            'bls': 'CUSR0000SERA01',
            'name': 'Televisions',
            'name_kr': '텔레비전',
            'parent': 'video_audio'
        },
        'cable_satellite_streaming': {
            'bls': 'CUSR0000SERA02',
            'name': 'Cable, satellite, and live streaming television service',
            'name_kr': '케이블·위성·스트리밍 TV',
            'parent': 'video_audio'
        },
        'other_video_equipment': {
            'bls': 'CUSR0000SERA03',
            'name': 'Other video equipment',
            'name_kr': '기타 비디오 장비',
            'parent': 'video_audio'
        },
        'audio_equipment': {
            'bls': 'CUSR0000SERA05',
            'name': 'Audio equipment',
            'name_kr': '오디오 장비',
            'parent': 'video_audio'
        },
        'video_audio_products': {
            'bls': 'CUSR0000SERAC',
            'name': 'Video and audio products',
            'name_kr': '비디오·오디오 제품',
            'parent': 'video_audio'
        },
        'video_audio_services': {
            'bls': 'CUSR0000SERAS',
            'name': 'Video and audio services',
            'name_kr': '비디오·오디오 서비스',
            'parent': 'video_audio'
        },
        'pets_pet_products_only': {
            'bls': 'CUSR0000SERB01',
            'name': 'Pets and pet products',
            'name_kr': '애완동물·용품만',
            'parent': 'pets_pet_products'
        },
        'pet_services_veterinary': {
            'bls': 'CUSR0000SERB02',
            'name': 'Pet services including veterinary',
            'name_kr': '애완동물 서비스',
            'parent': 'pets_pet_products'
        },
        'sports_vehicles_bicycles': {
            'bls': 'CUSR0000SERC01',
            'name': 'Sports vehicles including bicycles',
            'name_kr': '스포츠 차량·자전거',
            'parent': 'sporting_goods'
        },
        'sports_equipment': {
            'bls': 'CUSR0000SERC02',
            'name': 'Sports equipment',
            'name_kr': '스포츠 장비',
            'parent': 'sporting_goods'
        },
        'photographic_equipment_supplies': {
            'bls': 'CUSR0000SERD01',
            'name': 'Photographic equipment and supplies',
            'name_kr': '사진 장비·용품',
            'parent': 'photography'
        },
        'toys': {
            'bls': 'CUSR0000SERE01',
            'name': 'Toys',
            'name_kr': '장난감',
            'parent': 'other_recreational_goods'
        },
        'sewing_machines_fabric': {
            'bls': 'CUSR0000SERE02',
            'name': 'Sewing machines, fabric and supplies',
            'name_kr': '재봉틀·원단',
            'parent': 'other_recreational_goods'
        },
        'music_instruments_accessories': {
            'bls': 'CUSR0000SERE03',
            'name': 'Music instruments and accessories',
            'name_kr': '악기·액세서리',
            'parent': 'other_recreational_goods'
        },
        'club_membership_participant_sports': {
            'bls': 'CUSR0000SERF01',
            'name': 'Club membership for shopping clubs, fraternal, or other organizations, or participant sports fees',
            'name_kr': '클럽 회원권·스포츠 참가비',
            'parent': 'other_recreation_services'
        },
        'admissions': {
            'bls': 'CUSR0000SERF02',
            'name': 'Admissions',
            'name_kr': '입장료',
            'parent': 'other_recreation_services'
        },
        'fees_lessons_instructions': {
            'bls': 'CUSR0000SERF03',
            'name': 'Fees for lessons or instructions',
            'name_kr': '레슨·강습비',
            'parent': 'other_recreation_services'
        },
        'toys_games_hobbies': {
            'bls': 'CUSR0000SS61011',
            'name': 'Toys, games, hobbies and playground equipment',
            'name_kr': '장난감·게임·취미',
            'parent': 'other_recreational_goods'
        },
        'photographic_equipment_only': {
            'bls': 'CUSR0000SS61023',
            'name': 'Photographic equipment',
            'name_kr': '사진 장비',
            'parent': 'photography'
        },
        'pet_food_treats': {
            'bls': 'CUSR0000SS61031',
            'name': 'Pet food and treats',
            'name_kr': '애완동물 사료·간식',
            'parent': 'pets_pet_products'
        },
        'purchase_pets_supplies': {
            'bls': 'CUSR0000SS61032',
            'name': 'Purchase of pets, pet supplies, accessories',
            'name_kr': '애완동물·용품 구매',
            'parent': 'pets_pet_products'
        },
        'admission_movies_theaters_concerts': {
            'bls': 'CUSR0000SS62031',
            'name': 'Admission to movies, theaters, and concerts',
            'name_kr': '영화·연극·콘서트',
            'parent': 'admissions'
        },
        'admission_sporting_events': {
            'bls': 'CUSR0000SS62032',
            'name': 'Admission to sporting events',
            'name_kr': '스포츠 경기 입장료',
            'parent': 'admissions'
        },
        'pet_services_only': {
            'bls': 'CUSR0000SS62053',
            'name': 'Pet services',
            'name_kr': '애완동물 서비스',
            'parent': 'pet_services_veterinary'
        },
        'veterinarian_services': {
            'bls': 'CUSR0000SS62054',
            'name': 'Veterinarian services',
            'name_kr': '수의사 서비스',
            'parent': 'pet_services_veterinary'
        },
        
        # 기타 상품·서비스 세부
        'cigarettes': {
            'bls': 'CUSR0000SEGA01',
            'name': 'Cigarettes',
            'name_kr': '담배',
            'parent': 'tobacco'
        },
        'legal_services': {
            'bls': 'CUSR0000SEGD01',
            'name': 'Legal services',
            'name_kr': '법무 서비스',
            'parent': 'misc_personal_services'
        },
        'funeral_expenses': {
            'bls': 'CUSR0000SEGD02',
            'name': 'Funeral expenses',
            'name_kr': '장례비',
            'parent': 'misc_personal_services'
        },
        'laundry_dry_cleaning': {
            'bls': 'CUSR0000SEGD03',
            'name': 'Laundry and dry cleaning services',
            'name_kr': '세탁·드라이클리닝',
            'parent': 'misc_personal_services'
        },
        'financial_services': {
            'bls': 'CUSR0000SEGD05',
            'name': 'Financial services',
            'name_kr': '금융 서비스',
            'parent': 'misc_personal_services'
        },
        'tax_return_accounting': {
            'bls': 'CUSR0000SS68023',
            'name': 'Tax return preparation and other accounting fees',
            'name_kr': '세무·회계 서비스',
            'parent': 'misc_personal_services'
        }
    }
}

# 간단한 접근을 위한 BLS 시리즈 맵핑
ALL_BLS_SERIES_MAP = {}
for level_name, level_data in COMPLETE_ALL_CPI_HIERARCHY.items():
    for series_key, series_info in level_data.items():
        ALL_BLS_SERIES_MAP[series_key] = series_info['bls']

# 한국어 명칭 맵핑
ALL_KOREAN_NAMES = {}
for level_name, level_data in COMPLETE_ALL_CPI_HIERARCHY.items():
    for series_key, series_info in level_data.items():
        ALL_KOREAN_NAMES[series_key] = series_info['name_kr']

print(f"✅ 확장된 CPI 계층 구조 정의 완료!")
print(f"   - Level 1: {len(COMPLETE_ALL_CPI_HIERARCHY['level1'])}개 시리즈")
print(f"   - Level 2: {len(COMPLETE_ALL_CPI_HIERARCHY['level2'])}개 시리즈")  
print(f"   - Level 3: {len(COMPLETE_ALL_CPI_HIERARCHY['level3'])}개 시리즈")
print(f"   - Level 4: {len(COMPLETE_ALL_CPI_HIERARCHY['level4'])}개 시리즈")
print(f"   - Level 5: {len(COMPLETE_ALL_CPI_HIERARCHY['level5'])}개 시리즈")
print(f"   - Level 6: {len(COMPLETE_ALL_CPI_HIERARCHY['level6'])}개 시리즈")
print(f"   - Level 7: {len(COMPLETE_ALL_CPI_HIERARCHY['level7'])}개 시리즈")
print(f"   - Level 8: {len(COMPLETE_ALL_CPI_HIERARCHY['level8'])}개 시리즈")
print(f"   - 현재 총 {len(ALL_BLS_SERIES_MAP)}개 시리즈")
print("   - 목표: 325개 시리즈 완성")

# %%
def add_remaining_series():
    """남은 시리즈들을 추가하는 함수"""
    
    # 325개 모든 시리즈 ID 목록 (제공된 텍스트에서 추출)
    all_series_from_text = [
        'CUSR0000SA0', 'CUSR0000SA0E', 'CUSR0000SA0L1', 'CUSR0000SA0L12', 'CUSR0000SA0L12E',
        'CUSR0000SA0L12E4', 'CUSR0000SA0L1E', 'CUSR0000SA0L2', 'CUSR0000SA0L5', 'CUSR0000SA0LE',
        'CUSR0000SA311', 'CUSR0000SAA', 'CUSR0000SAA1', 'CUSR0000SAA2', 'CUSR0000SAC',
        'CUSR0000SACE', 'CUSR0000SACL1', 'CUSR0000SACL11', 'CUSR0000SACL1E', 'CUSR0000SACL1E4',
        'CUSR0000SAD', 'CUSR0000SAE', 'CUSR0000SAE1', 'CUSR0000SAE2', 'CUSR0000SAE21',
        'CUSR0000SAEC', 'CUSR0000SAES', 'CUSR0000SAF', 'CUSR0000SAF1', 'CUSR0000SAF11',
        'CUSR0000SAF111', 'CUSR0000SAF112', 'CUSR0000SAF1121', 'CUSR0000SAF11211', 'CUSR0000SAF113',
        'CUSR0000SAF1131', 'CUSR0000SAF114', 'CUSR0000SAF115', 'CUSR0000SAF116', 'CUSR0000SAG',
        'CUSR0000SAG1', 'CUSR0000SAGC', 'CUSR0000SAH', 'CUSR0000SAH1', 'CUSR0000SAH2',
        'CUSR0000SAH21', 'CUSR0000SAH3', 'CUSR0000SAH31', 'CUSR0000SAM', 'CUSR0000SAM1',
        'CUSR0000SAM2', 'CUSR0000SAN', 'CUSR0000SAN1D', 'CUSR0000SANL1', 'CUSR0000SANL11',
        'CUSR0000SANL113', 'CUSR0000SANL13', 'CUSR0000SAR', 'CUSR0000SARC', 'CUSR0000SARS',
        'CUSR0000SAS', 'CUSR0000SAS24', 'CUSR0000SAS2RS', 'CUSR0000SAS367', 'CUSR0000SAS4',
        'CUSR0000SASL2RS', 'CUSR0000SASL5', 'CUSR0000SASLE', 'CUSR0000SAT', 'CUSR0000SAT1',
        'CUSR0000SATCLTB', 'CUSR0000SEAA', 'CUSR0000SEAA01', 'CUSR0000SEAA02', 'CUSR0000SEAA03',
        'CUSR0000SEAA04', 'CUSR0000SEAB', 'CUSR0000SEAC', 'CUSR0000SEAC01', 'CUSR0000SEAC02',
        'CUSR0000SEAC03', 'CUSR0000SEAC04', 'CUSR0000SEAD', 'CUSR0000SEAE', 'CUSR0000SEAE01',
        'CUSR0000SEAE02', 'CUSR0000SEAE03', 'CUSR0000SEAF', 'CUSR0000SEAG', 'CUSR0000SEAG01',
        'CUSR0000SEAG02', 'CUSR0000SEEA', 'CUSR0000SEEB', 'CUSR0000SEEB01', 'CUSR0000SEEB02',
        'CUSR0000SEEB03', 'CUSR0000SEEB04', 'CUSR0000SEEC', 'CUSR0000SEEC01', 'CUSR0000SEEC02',
        'CUSR0000SEEE', 'CUSR0000SEEE01', 'CUSR0000SEEE03', 'CUSR0000SEEE04', 'CUSR0000SEEEC',
        'CUSR0000SEFA', 'CUSR0000SEFA01', 'CUSR0000SEFA02', 'CUSR0000SEFA03', 'CUSR0000SEFB',
        'CUSR0000SEFB01', 'CUSR0000SEFB02', 'CUSR0000SEFB03', 'CUSR0000SEFB04', 'CUSR0000SEFC',
        'CUSR0000SEFC01', 'CUSR0000SEFC02', 'CUSR0000SEFC03', 'CUSR0000SEFD', 'CUSR0000SEFD01',
        'CUSR0000SEFD02', 'CUSR0000SEFD03', 'CUSR0000SEFD04', 'CUSR0000SEFE', 'CUSR0000SEFF',
        'CUSR0000SEFF01', 'CUSR0000SEFF02', 'CUSR0000SEFG', 'CUSR0000SEFG01', 'CUSR0000SEFG02',
        'CUSR0000SEFH', 'CUSR0000SEFJ', 'CUSR0000SEFJ01', 'CUSR0000SEFJ02', 'CUSR0000SEFJ03',
        'CUSR0000SEFJ04', 'CUSR0000SEFK', 'CUSR0000SEFK01', 'CUSR0000SEFK02', 'CUSR0000SEFK03',
        'CUSR0000SEFK04', 'CUSR0000SEFL', 'CUSR0000SEFL01', 'CUSR0000SEFL02', 'CUSR0000SEFL03',
        'CUSR0000SEFL04', 'CUSR0000SEFM', 'CUSR0000SEFM01', 'CUSR0000SEFM02', 'CUSR0000SEFM03',
        'CUSR0000SEFN', 'CUSR0000SEFN01', 'CUSR0000SEFN03', 'CUSR0000SEFP', 'CUSR0000SEFP01',
        'CUSR0000SEFP02', 'CUSR0000SEFR', 'CUSR0000SEFR01', 'CUSR0000SEFR02', 'CUSR0000SEFR03',
        'CUSR0000SEFS', 'CUSR0000SEFS01', 'CUSR0000SEFS02', 'CUSR0000SEFS03', 'CUSR0000SEFT',
        'CUSR0000SEFT01', 'CUSR0000SEFT02', 'CUSR0000SEFT03', 'CUSR0000SEFT04', 'CUSR0000SEFT06',
        'CUSR0000SEFV', 'CUSR0000SEFV01', 'CUSR0000SEFV03', 'CUSR0000SEFV05', 'CUSR0000SEFW',
        'CUSR0000SEFW01', 'CUSR0000SEFW02', 'CUSR0000SEFW03', 'CUSR0000SEFX', 'CUSR0000SEGA',
        'CUSR0000SEGA01', 'CUSR0000SEGD', 'CUSR0000SEGD01', 'CUSR0000SEGD02', 'CUSR0000SEGD03',
        'CUSR0000SEGD05', 'CUSR0000SEGE', 'CUSR0000SEHA', 'CUSR0000SEHB', 'CUSR0000SEHB01',
        'CUSR0000SEHB02', 'CUSR0000SEHC', 'CUSR0000SEHC01', 'CUSR0000SEHE', 'CUSR0000SEHE01',
        'CUSR0000SEHE02', 'CUSR0000SEHF', 'CUSR0000SEHF01', 'CUSR0000SEHF02', 'CUSR0000SEHG',
        'CUSR0000SEHG01', 'CUSR0000SEHG02', 'CUSR0000SEHH', 'CUSR0000SEHH02', 'CUSR0000SEHH03',
        'CUSR0000SEHJ', 'CUSR0000SEHJ03', 'CUSR0000SEHK', 'CUSR0000SEHK01', 'CUSR0000SEHK02',
        'CUSR0000SEHL', 'CUSR0000SEHL02', 'CUSR0000SEHL04', 'CUSR0000SEHM', 'CUSR0000SEHM01',
        'CUSR0000SEHM02', 'CUSR0000SEHN', 'CUSR0000SEHN01', 'CUSR0000SEHN03', 'CUSR0000SEHP03',
        'CUSR0000SEMC', 'CUSR0000SEMC01', 'CUSR0000SEMC02', 'CUSR0000SEMC03', 'CUSR0000SEMC04',
        'CUSR0000SEMD', 'CUSR0000SEMD01', 'CUSR0000SEMD02', 'CUSR0000SEMF', 'CUSR0000SEMF01',
        'CUSR0000SEMF02', 'CUSR0000SERA', 'CUSR0000SERA01', 'CUSR0000SERA02', 'CUSR0000SERA03',
        'CUSR0000SERA05', 'CUSR0000SERAC', 'CUSR0000SERAS', 'CUSR0000SERB', 'CUSR0000SERB01',
        'CUSR0000SERB02', 'CUSR0000SERC', 'CUSR0000SERC01', 'CUSR0000SERC02', 'CUSR0000SERD',
        'CUSR0000SERD01', 'CUSR0000SERE', 'CUSR0000SERE01', 'CUSR0000SERE02', 'CUSR0000SERE03',
        'CUSR0000SERF', 'CUSR0000SERF01', 'CUSR0000SERF02', 'CUSR0000SERF03', 'CUSR0000SERG',
        'CUSR0000SETA', 'CUSR0000SETA01', 'CUSR0000SETA02', 'CUSR0000SETA03', 'CUSR0000SETA04',
        'CUSR0000SETB', 'CUSR0000SETB01', 'CUSR0000SETB02', 'CUSR0000SETC', 'CUSR0000SETC01',
        'CUSR0000SETD', 'CUSR0000SETD03', 'CUSR0000SETE', 'CUSR0000SETF03', 'CUSR0000SETG',
        'CUSR0000SETG01', 'CUSR0000SETG02', 'CUSR0000SS02042', 'CUSR0000SS0206A', 'CUSR0000SS0206B',
        'CUSR0000SS04011', 'CUSR0000SS04012', 'CUSR0000SS04031', 'CUSR0000SS05011', 'CUSR0000SS0501A',
        'CUSR0000SS06011', 'CUSR0000SS06021', 'CUSR0000SS07011', 'CUSR0000SS07021', 'CUSR0000SS09011',
        'CUSR0000SS09021', 'CUSR0000SS10011', 'CUSR0000SS11031', 'CUSR0000SS13031', 'CUSR0000SS14011',
        'CUSR0000SS14021', 'CUSR0000SS16011', 'CUSR0000SS17031', 'CUSR0000SS18041', 'CUSR0000SS18042',
        'CUSR0000SS18043', 'CUSR0000SS1804B', 'CUSR0000SS18064', 'CUSR0000SS20021', 'CUSR0000SS20022',
        'CUSR0000SS20053', 'CUSR0000SS30021', 'CUSR0000SS33032', 'CUSR0000SS45011', 'CUSR0000SS4501A',
        'CUSR0000SS45021', 'CUSR0000SS45031', 'CUSR0000SS47014', 'CUSR0000SS47015', 'CUSR0000SS47016',
        'CUSR0000SS52051', 'CUSR0000SS53022', 'CUSR0000SS53023', 'CUSR0000SS5702', 'CUSR0000SS5703',
        'CUSR0000SS61011', 'CUSR0000SS61023', 'CUSR0000SS61031', 'CUSR0000SS61032', 'CUSR0000SS62031',
        'CUSR0000SS62032', 'CUSR0000SS62053', 'CUSR0000SS62054', 'CUSR0000SS68023', 'CUSR0000SSFV031A'
    ]
    
    # 현재 정의된 시리즈들
    current_series = set(ALL_BLS_SERIES_MAP.values())
    
    # 누락된 시리즈들
    missing_series = [s for s in all_series_from_text if s not in current_series]
    
    print(f"현재 정의된 시리즈: {len(current_series)}개")
    print(f"목표 시리즈: {len(all_series_from_text)}개")
    print(f"누락된 시리즈: {len(missing_series)}개")
    
    if missing_series:
        print("누락된 시리즈들:")
        for series in missing_series[:20]:  # 처음 20개만 표시
            print(f"  {series}")
        if len(missing_series) > 20:
            print(f"  ... 그 외 {len(missing_series) - 20}개")
    
    return missing_series

# 누락된 시리즈 확인
missing = add_remaining_series()

# %%
def get_series_by_level(level):
    """특정 레벨의 모든 시리즈 키 반환"""
    if level not in COMPLETE_ALL_CPI_HIERARCHY:
        print(f"⚠️ 잘못된 레벨: {level}")
        return []
    return list(COMPLETE_ALL_CPI_HIERARCHY[level].keys())

def get_children_series(parent_key):
    """특정 부모의 자식 시리즈들 반환"""
    children = []
    for level_data in COMPLETE_ALL_CPI_HIERARCHY.values():
        for key, info in level_data.items():
            if info.get('parent') == parent_key:
                children.append(key)
    return children

def get_korean_name(series_key):
    """시리즈 키의 한국어 명칭 반환"""
    return ALL_KOREAN_NAMES.get(series_key, series_key)

def get_bls_id(series_key):
    """시리즈 키의 BLS ID 반환"""
    return ALL_BLS_SERIES_MAP.get(series_key, None)

def show_hierarchy_summary():
    """계층 구조 요약 표시"""
    print("=== 확장된 CPI 계층 구조 요약 ===\n")
    
    for level, level_data in COMPLETE_ALL_CPI_HIERARCHY.items():
        level_names = {
            'level1': 'Level 1: 최상위 종합지표',
            'level2': 'Level 2: 상품/서비스 구분', 
            'level3': 'Level 3: 주요 생활비 카테고리',
            'level4': 'Level 4: 세부 카테고리',
            'level5': 'Level 5: 더 세부적인 분류',
            'level6': 'Level 6: 가장 세부적인 분류',
            'level7': 'Level 7: 매우 세부적인 분류',
            'level8': 'Level 8: 개별 품목'
        }
        
        print(f"### {level_names[level]} ({len(level_data)}개) ###")
        for key, info in level_data.items():
            korean_name = info.get('name_kr', info['name'])
            parent_info = f" → {info['parent']}" if 'parent' in info else ""
            print(f"  {key}: {korean_name}{parent_info}")
        print()

# %%
# 계층 구조 요약 표시
show_hierarchy_summary()

# %%
print("=== 유틸리티 함수 테스트 ===")
print(f"Level 3 시리즈들: {get_series_by_level('level3')}")
print(f"Housing의 자식들: {get_children_series('housing')}")
print(f"gasoline 한국어: {get_korean_name('gasoline')}")
print(f"gasoline BLS ID: {get_bls_id('gasoline')}")