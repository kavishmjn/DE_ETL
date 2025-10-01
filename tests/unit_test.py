def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"


def test_channel_handle(channel_handle):
    assert channel_handle == 'WWE'

def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert  conn.login == 'mock_username'
    assert  conn.password =='1234567890'
    assert  conn.host == 'mock_host'
    assert  conn.port == 1234
    assert  conn.schema == 'mock_db_name'



def test_dags_integrity(dagbag):
    #1.0
    assert dagbag.import_errors == {},f"Import errors found: {dagbag.import_errors}"
    print("==="*8)
    print(dagbag.import_errors)
    
    #2.0
    expected_dag_ids = ["data_quality","update_db",'produce_json']
    loaded_dag_ids = list(dagbag.dags.keys())
    print("==="*8)
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing"

    #3.0
    assert dagbag.size()==3
    print("==="*8)
    print(dagbag.size())

    #4.0
    expected_tasks_counts = {
        "data_quality":2,
        "update_db":2,
        "produce_json":4
    }
    print("==="*8)
    for dag_id,dag in dagbag.dags.items():
        expected_count = expected_tasks_counts[dag_id]
        actual_count = len(dag.tasks)
        assert expected_count==actual_count,f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."
        print(dag_id,len(dag.tasks))
        


