import streamlit as st
import duckdb
import pandas as pd
import os

# DB 파일 경로 설정 (GitHub 리포지토리의 루트에 있어야 합니다)
DB_FILE_PATH = "madang.db" 

# 1. DuckDB 연결을 st.cache_resource로 캐시합니다.
# read_only=False로 설정하여 쓰기 작업을 허용합니다.
@st.cache_resource
def get_duckdb_connection():
    try:
        # read_only=False (쓰기 모드)로 연결
        conn = duckdb.connect(database=DB_FILE_PATH, read_only=False)
        return conn
    except Exception as e:
        st.error(f"DuckDB 연결 오류: {e}")
        return None

conn = get_duckdb_connection()

# 2. DML (INSERT 등) 실행 및 캐시 클리어 함수
def run_dml(sql):
    if conn is None:
        return False
    try:
        conn.execute(sql)
        # 데이터가 변경되었으므로, SELECT 쿼리 결과를 새로고침하기 위해 캐시를 지웁니다.
        st.cache_data.clear() 
        return True
    except Exception as e:
        st.error(f"쿼리 실행 중 오류 발생. SQL 구문을 확인하세요: {e}")
        return False

# 3. SELECT 쿼리 실행 및 캐시 적용 함수
# 삽입 후 최신 데이터를 즉시 보여주기 위해 사용합니다.
@st.cache_data(ttl=60) # 1분 캐싱 (데이터가 자주 바뀌지 않는다면 TTL을 더 늘릴 수 있습니다)
def get_customer_data():
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute("SELECT * FROM Customer ORDER BY custid").df()
    except Exception as e:
        st.error(f"테이블 조회 오류: {e}")
        return pd.DataFrame()


# ----------------- Streamlit UI -----------------

st.title("DuckDB 고객 데이터 삽입 앱")

if conn:
    # --- 데이터 삽입 섹션 (Form 사용) ---
    st.header("새로운 고객 정보 삽입")
    
    # clear_on_submit=True 설정으로 삽입 후 폼 입력 필드를 초기화합니다.
    with st.form("insert_form", clear_on_submit=True):
        st.markdown("##### 고객 정보 입력")
        
        # custid, name, address 컬럼에 맞춰 입력 필드 구성
        new_custid = st.number_input("CustID (숫자)", min_value=1, value=6, step=1)
        new_name = st.text_input("Name (이름)", "장준희")
        new_address = st.text_input("Address (주소)", "인천광역시")
        
        submitted = st.form_submit_button("데이터 삽입 (INSERT)")
        
        if submitted:
            # 올바른 SQL 구문으로 INSERT 쿼리 생성 (VALUES, 작은따옴표)
            insert_sql = f"""
            INSERT INTO Customer (custid, name, address) 
            VALUES ({new_custid}, '{new_name}', '{new_address}');
            """
            
            if run_dml(insert_sql):
                st.success(f"CustID {new_custid} 고객 정보가 삽입되었습니다.")
            
    # --- 데이터 확인 섹션 ---
    st.header("Customer 테이블 현재 데이터")
    
    customer_df = get_customer_data()
    st.dataframe(customer_df)

else:
    st.error("DuckDB 연결이 실패하여 앱을 사용할 수 없습니다.")