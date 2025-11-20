import streamlit as st
import duckdb
import pandas as pd
import os

DB_FILE_PATH = "madang.db"

def get_duckdb_connection():
    try:
        return duckdb.connect(database=DB_FILE_PATH, read_only=True)
    except Exception as e:
        st.error(f"DuckDB 연결 오류: {e}")
        st.info(f"'{DB_FILE_PATH}' 파일이 GitHub 리포지토리에 있는지 확인하세요.")
        return None

conn = get_duckdb_connection()

def run_query(sql, return_type='df'):
    if conn is None:
        return pd.DataFrame() # 연결 오류 시 빈 데이터프레임 반환
    
    # st.cache_data를 사용하여 쿼리 결과를 캐시합니다.
    # SQL 쿼리가 변경되지 않는 한, 결과를 다시 계산하지 않습니다.
    @st.cache_data(show_spinner=f"쿼리 실행 중: {sql.split(' ', 2)[2].split(' ', 1)[0]}...")
    def execute_query(sql, return_type):
        if return_type == 'df':
            return conn.execute(sql).df()
        else:
            return conn.execute(sql).fetchall()

    return execute_query(sql, return_type)


# ----------------- Streamlit UI -----------------

st.title("DuckDB 'Madang' 데이터 분석 📚")

if conn:
    st.subheader("Customer 테이블 데이터")
    customer_df = run_query("select * from Customer", "df")
    st.dataframe(customer_df)

    st.subheader("Book 테이블 데이터")
    book_df = run_query("select * from Book", "df")
    st.dataframe(book_df)
    
    # --- 조인 쿼리 예시 ---
    st.subheader("Customer-Orders 조인 결과")
    join_query = """
    SELECT 
        c.name, b.bookname, o.saleprice, o.orderdate 
    FROM Orders o
    JOIN Customer c ON c.custid = o.custid
    JOIN Book b ON b.bookid = o.bookid;
    """
    join_df = run_query(join_query)
    st.dataframe(join_df)

    # --- 사용자 입력 쿼리 (선택 사항) ---
    st.subheader("직접 쿼리 실행하기")
    user_query = st.text_area("SQL 쿼리를 입력하세요:", "SELECT name, address FROM Customer WHERE custid = 1;")
    if st.button("쿼리 실행"):
        try:
            custom_result = run_query(user_query, "df")
            st.dataframe(custom_result)
        except Exception as e:
            st.error(f"쿼리 실행 중 오류 발생: {e}")