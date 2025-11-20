import streamlit as st
import duckdb
import pandas as pd
from datetime import datetime
import time # time 모듈은 datetime으로 대체하는 것이 더 좋습니다.

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
        st.code(sql, language='sql')
        return False

# 3. SELECT 쿼리 실행 및 캐시 적용 함수 (기존 코드 유지)
# ... (get_customer_data, get_Orders_data, get_book_data 함수는 그대로 유지합니다.)
@st.cache_data(ttl=60) # 1분 캐싱
def get_customer_data():
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute("SELECT * FROM Customer ORDER BY custid").df()
    except Exception as e:
        st.error(f"테이블 조회 오류: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60) # 1분 캐싱
def get_Orders_data():
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute("SELECT * FROM Orders ORDER BY custid").df()
    except Exception as e:
        st.error(f"테이블 조회 오류: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60) # 1분 캐싱
def get_book_data():
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute("SELECT * FROM Book ORDER BY bookid").df()
    except Exception as e:
        st.error(f"테이블 조회 오류: {e}")
        return pd.DataFrame()

# ----------------- 새로운 기능: 다음 OrderID 계산 함수 -----------------

def get_next_order_id():
    """Orders 테이블에서 최대 orderid를 조회하고 1을 더한 값을 반환합니다."""
    if conn is None:
        return None
    try:
        # SQL을 사용하여 최대 orderid를 조회하고 1을 더합니다.
        # MAX(orderid)가 NULL일 경우 (테이블이 비어있을 경우) 1을 반환하도록 COALESCE를 사용합니다.
        result = conn.execute("SELECT COALESCE(MAX(orderid), 0) + 1 FROM Orders").fetchone()
        return result[0]
    except Exception as e:
        st.error(f"다음 OrderID 조회 오류: {e}")
        return None

# ----------------- Streamlit UI -----------------

st.title("DuckDB 'madang db'")

if conn:
    # --- 고객 별 주문 조회 섹션 ---
    st.header("고객 별 주문 조회")

    with st.form("select_form"):
        st.markdown("##### 고객 ID를 선택하여 해당 고객의 주문 내역을 조회하세요.")
        
        customer_df = get_customer_data()
        custid_options = customer_df['custid'].tolist()
        
        # custid_options가 비어있지 않은지 확인 (최소 하나의 고객이 있어야 함)
        if not custid_options:
            st.warning("Customer 테이블에 데이터가 없습니다. 먼저 고객을 삽입하세요.")
            selected_custid = None
        else:
            selected_custid = st.selectbox("고객 ID 선택", custid_options)
        
        submitted = st.form_submit_button("주문 내역 조회")
        
        if submitted and selected_custid is not None:
            try:
                orders_df = conn.execute(f"""
                SELECT * FROM Orders WHERE custid = {selected_custid} ORDER BY orderid
                """).df()
                
                st.subheader(f"CustID {selected_custid} 고객의 주문 내역")
                st.dataframe(orders_df)
            except Exception as e:
                st.error(f"주문 내역 조회 오류: {e}")

    # --- 데이터 삽입 섹션 (Customer) ---
    st.header("새로운 고객 정보 삽입")
    # ... (Customer 삽입 코드는 그대로 유지)
    with st.form("insert_form", clear_on_submit=True):
        st.markdown("##### 고객 정보 입력")
        
        # 현재 최대 custid를 조회하여 다음 번호 기본값으로 설정하면 더 편리합니다.
        try:
            next_custid = conn.execute("SELECT COALESCE(MAX(custid), 0) + 1 FROM Customer").fetchone()[0]
        except:
            next_custid = 6 # 오류 시 기본값
            
        new_custid = st.number_input("CustID (숫자)", min_value=1, value=next_custid, step=1)
        new_name = st.text_input("Name (이름)", "장준희")
        new_address = st.text_input("Address (주소)", "인천광역시")
        new_phone = st.text_input("Phone (전화번호)", "010-1234-5678")
        
        submitted = st.form_submit_button("고객 정보 삽입 (INSERT)")
        
        if submitted:
            insert_sql = f"""
            INSERT INTO Customer (custid, name, address, phone) 
            VALUES ({new_custid}, '{new_name}', '{new_address}', '{new_phone}');
            """
            
            if run_dml(insert_sql):
                st.success(f"CustID {new_custid} 고객 정보가 삽입되었습니다.")

    # --- 주문 정보 삽입 섹션 (Orders) ---
    st.header("주문 정보 삽입")
    with st.form("inesrt_order_form", clear_on_submit=True):
        st.markdown("##### 주문 정보 입력")
        
        # **수정된 부분:** orderid 자동 생성 로직 추가
        next_order_id = get_next_order_id()
        st.info(f"다음 OrderID는 **{next_order_id}**로 자동 설정됩니다.")
        
        # OrderID는 자동 생성되므로 입력 필드는 제거하고 변수로 사용합니다.
        
        # Book ID 선택을 위해 Book 테이블 데이터를 사용하면 편리합니다.
        book_df = get_book_data()
        bookid_options = book_df['bookid'].tolist()
        
        order_custid = st.number_input("CustID (숫자)", min_value=1, value=1, step=1, key="order_custid")
        
        selected_bookid = st.selectbox("도서 ID 선택", bookid_options, key="order_bookid") 
        
        # 선택된 bookid의 가격을 기본값으로 설정 (Book 테이블에 saleprice 컬럼이 있다면)
        try:
             # Book 테이블에 saleprice 컬럼이 없으므로, saleprice는 직접 입력하도록 유지
             # 또는 Book 테이블의 price 컬럼을 사용하는 로직 추가 가능 (현재는 생략)
            pass
        except:
            pass
            
        order_saleprice = st.number_input("판매가격", min_value=0, value=10000, step=1000, key="order_saleprice")
        
        # 현재 날짜를 문자열로 포맷 (SQL에 맞게)
        order_date_str = datetime.now().strftime("'%Y-%m-%d'") 
        
        # 날짜 입력 필드를 추가할 수도 있지만, 현재 코드는 time.strftime을 사용하고 있어 datetime으로 변경
        st.caption(f"주문 날짜는 현재 날짜로 자동 설정됩니다.")

        submitted = st.form_submit_button("주문 정보 삽입 (INSERT)")
        
        if submitted and next_order_id is not None:
            # **수정된 부분:** INSERT 문에 orderid 포함
            insert_order_sql = f"""
            INSERT INTO Orders (orderid, custid, bookid, saleprice, orderdate) 
            VALUES ({next_order_id}, {order_custid}, {selected_bookid}, {order_saleprice}, {order_date_str});
            """
            
            if run_dml(insert_order_sql):
                st.success(f"Order ID **{next_order_id}**의 주문 정보가 성공적으로 삽입되었습니다.")
        elif submitted and next_order_id is None:
            st.error("OrderID를 생성할 수 없습니다. DB 연결 상태를 확인하세요.")
    
            
    # --- 데이터 확인 섹션 (기존 코드 유지) ---
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