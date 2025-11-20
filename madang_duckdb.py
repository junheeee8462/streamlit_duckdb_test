import streamlit as st
import duckdb
import pandas as pd
import time

# ----------------- DuckDB 연결 및 쿼리 함수 정의 -----------------
DB_FILE_PATH = "madang.db" 

@st.cache_resource
def get_duckdb_connection():
    try:
        conn = duckdb.connect(database=DB_FILE_PATH, read_only=False)
        return conn
    except Exception as e:
        st.error(f"DuckDB 연결 오류: {e}")
        return None

conn = get_duckdb_connection()

def run_dml(sql):
    if conn is None:
        return False
    try:
        conn.execute(sql)
        st.cache_data.clear() 
        return True
    except Exception as e:
        st.error(f"쿼리 실행 중 오류 발생. SQL 구문을 확인하세요: {e}")
        return False

# 3. SELECT 쿼리 실행 및 캐시 적용 함수
@st.cache_data(ttl=60) # 1분 캐싱
def get_customer_data():
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute("SELECT * FROM Customer ORDER BY custid").df()
    except Exception as e:
        st.error(f"테이블 조회 오류: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60) # 1분 캐싱 (데이터가 자주 바뀌지 않는다면 TTL을 더 늘릴 수 있습니다)
def get_Orders_data():
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute("SELECT * FROM Orders ORDER BY custid").df()
    except Exception as e:
        st.error(f"테이블 조회 오류: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60) # 1분 캐싱 (데이터가 자주 바뀌지 않는다면 TTL을 더 늘릴 수 있습니다)
def get_book_data():
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute("SELECT * FROM Book ORDER BY bookid").df()
    except Exception as e:
        st.error(f"테이블 조회 오류: {e}")
        return pd.DataFrame()



# ----------------- Streamlit UI -----------------

st.title("DuckDB 'madang db'")

if conn:
    # --- 고객 별 주문 조회 섹션 ---
    st.header("고객 별 주문 조회")

    with st.form("select_form"):
        st.markdown("##### 고객 ID를 선택하여 해당 고객의 주문 내역을 조회하세요.")
        
        customer_df = get_customer_data()
        custid_options = customer_df['custid'].tolist()
        
        selected_custid = st.selectbox("고객 ID 선택", custid_options)
        
        submitted = st.form_submit_button("주문 내역 조회")
        
        if submitted:
            try:
                orders_df = conn.execute(f"""
                SELECT * FROM Orders WHERE custid = {selected_custid} ORDER BY orderid
                """).df()
                
                st.subheader(f"CustID {selected_custid} 고객의 주문 내역")
                st.dataframe(orders_df)
            except Exception as e:
                st.error(f"주문 내역 조회 오류: {e}")

    # --- 데이터 삽입 섹션 (Form 사용) ---
    st.header("새로운 고객 정보 삽입")
    
    with st.form("insert_form", clear_on_submit=True):
        st.markdown("##### 고객 정보 입력")
        
        new_custid = st.number_input("CustID (숫자)", min_value=1, value=6, step=1)
        new_name = st.text_input("Name (이름)", "장준희")
        new_address = st.text_input("Address (주소)", "인천광역시")
        new_phone = st.text_input("Phone (전화번호)", "010-1234-5678")  # Phone 필드 추가
        
        submitted = st.form_submit_button("데이터 삽입 (INSERT)")
        
        if submitted:
            # 올바른 SQL 구문으로 INSERT 쿼리 생성 (VALUES, 작은따옴표)
            insert_sql = f"""
            INSERT INTO Customer (custid, name, address, phone) 
            VALUES ({new_custid}, '{new_name}', '{new_address}', '{new_phone}');
            """
            
            if run_dml(insert_sql):
                st.success(f"CustID {new_custid} 고객 정보가 삽입되었습니다.")

    # --- 주문 정보 삽입 섹션 ---
    st.header("주문 정보 삽입")
    with st.form("inesrt_order_form", clear_on_submit=True):
        st.markdown("##### 주문 정보 입력")
        
        order_custid = st.number_input("CustID (숫자)", min_value=1, value=1, step=1)
        order_bookid = st.text_input("도서 번호")        
        order_date = time.strftime("'%Y-%m-%d'", time.localtime())  # 현재 날짜를 기본값으로 설정
        submitted = st.form_submit_button("주문 정보 삽입 (INSERT)")
        
        if submitted:
            insert_order_sql = f"""
            INSERT INTO Orders (custid, bookid, orderdate) 
            VALUES ({order_custid}, '{order_bookid}', {order_date});
            """
            
            if run_dml(insert_order_sql):
                st.success(f"CustID {order_custid} 고객의 주문 정보가 삽입되었습니다.")
    
            
    # --- 데이터 확인 섹션 ---
    st.header("Customer 테이블")
    
    customer_df = get_customer_data()
    st.dataframe(customer_df)

    st.header("Orders 테이블")
    orders_df = get_Orders_data()
    st.dataframe(orders_df)

    st.header("Book 테이블")
    book_df = get_book_data()
    st.dataframe(book_df)
else:
    st.error("DuckDB 연결이 실패하여 앱을 사용할 수 없습니다.")