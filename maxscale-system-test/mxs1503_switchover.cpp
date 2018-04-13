/**
 * MXS-1503: Master reconnection with MariaDBMon failover/switchover
 */
#include "testconnections.h"
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
    Mariadb_nodes::require_gtid(true);
    TestConnections test(argc, argv);

    cout << "1: Connection should survive two switchovers" << endl;
    test.maxscales->connect();

    for (int i = 2; i > 0; i--)
    {
        test.try_query(test.maxscales->conn_rwsplit[0], "SELECT @@last_insert_id");
        test.maxscales->ssh_node_f(0, true, "maxctrl call command mariadbmon switchover MySQL-Monitor server%d", i);
        sleep(5);
        test.try_query(test.maxscales->conn_rwsplit[0], "SELECT @@last_insert_id");
    }

    test.maxscales->disconnect();
    test.repl->fix_replication();

    cout << "2: Connection should survive a failover" << endl;
    test.maxscales->connect();

    test.try_query(test.maxscales->conn_rwsplit[0], "SELECT @@last_insert_id");

    test.repl->block_node(0);
    sleep(5);
    test.maxscales->ssh_node_f(0, true, "maxctrl call command mariadbmon MySQL-Monitor failover");
    sleep(5);

    test.try_query(test.maxscales->conn_rwsplit[0], "SELECT @@last_insert_id");

    test.maxscales->disconnect();

    // Clean up after testing
    test.repl->unblock_node(0);
    test.maxscales->ssh_node_f(0, true, "maxctrl call command mariadbmon MySQL-Monitor switchover server1");
    test.repl->fix_replication();

    return test.global_result;
}
