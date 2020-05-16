import psycopg2


REDSHIFT_CONFIG = {
    'region': "ap-south-1",
    'host': "vedantu.cbalxlkgcxvs.ap-south-1.redshift.amazonaws.com",
    'port': 5439,
    'username': "harkirat",
    'password': "Oa8kyL1OKL0WAxhcbp",
    'database': "vedantu"
}


def make_select_query_with_redshift(select_statement):
    redshift_conn = psycopg2.connect(host=REDSHIFT_CONFIG['host'],
                                     port=REDSHIFT_CONFIG['port'],
                                     user=REDSHIFT_CONFIG['username'],
                                     password=REDSHIFT_CONFIG['password'],
                                     dbname=REDSHIFT_CONFIG['database'])
    print("Redshift connection done")

    cursor = redshift_conn.cursor()

    cursor.execute(select_statement)
    data = cursor.fetchall()

    colnames = [desc[0] for desc in cursor.description]

    redshift_conn.close()

    return data, colnames
