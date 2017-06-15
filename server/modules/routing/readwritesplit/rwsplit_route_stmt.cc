/*
 * Copyright (c) 2016 MariaDB Corporation Ab
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2020-01-01
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */

#include "readwritesplit.hh"
#include "rwsplit_internal.hh"

#include <strings.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#include <maxscale/alloc.h>
#include <maxscale/router.h>
#include <maxscale/modutil.h>

/**
 * The functions that support the routing of queries to back end
 * servers. All the functions in this module are internal to the read
 * write split router, and not intended to be called from anywhere else.
 */

extern int (*criteria_cmpfun[LAST_CRITERIA])(const void *, const void *);

static SRWBackend get_root_master_bref(ROUTER_CLIENT_SES *rses);

/**
 * Find out which of the two backend servers has smaller value for select
 * criteria property.
 *
 * @param cand  previously selected candidate
 * @param new   challenger
 * @param sc    select criteria
 *
 * @return pointer to backend reference of that backend server which has smaller
 * value in selection criteria. If either reference pointer is NULL then the
 * other reference pointer value is returned.
 */
static SRWBackend& check_candidate_bref(SRWBackend& cand,
                                        SRWBackend& new_bref,
                                        select_criteria_t sc)
{
    int (*p)(const void *, const void *);
    /** get compare function */
    p = criteria_cmpfun[sc];

    if (new_bref == NULL)
    {
        return cand;
    }
    else if (cand == NULL || (p((void *)&cand, (void *)&new_bref) > 0))
    {
        return new_bref;
    }
    else
    {
        return cand;
    }
}

void handle_connection_keepalive(ROUTER_INSTANCE *inst, ROUTER_CLIENT_SES *rses,
                                 SRWBackend& target)
{
    ss_dassert(target);
    ss_debug(int nserv = 0);
    /** Each heartbeat is 1/10th of a second */
    int keepalive = inst->rwsplit_config.connection_keepalive * 10;

    for (SRWBackendList::iterator it = rses->backends.begin();
         it != rses->backends.end(); it++)
    {
        SRWBackend bref = *it;

        if (bref->in_use() && bref != target && !bref->is_waiting_result())
        {
            ss_debug(nserv++);
            int diff = hkheartbeat - bref->dcb()->last_read;

            if (diff > keepalive)
            {
                MXS_INFO("Pinging %s, idle for %d seconds",
                         bref->server()->unique_name, diff / 10);
                modutil_ignorable_ping(bref->dcb());
            }
        }
    }

    ss_dassert(nserv < rses->rses_nbackends);
}

/**
 * Routing function. Find out query type, backend type, and target DCB(s).
 * Then route query to found target(s).
 * @param inst      router instance
 * @param rses      router session
 * @param querybuf  GWBUF including the query
 *
 * @return true if routing succeed or if it failed due to unsupported query.
 * false if backend failure was encountered.
 */
bool route_single_stmt(ROUTER_INSTANCE *inst, ROUTER_CLIENT_SES *rses,
                       GWBUF *querybuf)
{
    uint32_t qtype = QUERY_TYPE_UNKNOWN;
    uint8_t packet_type;
    route_target_t route_target;
    bool succp = false;
    bool non_empty_packet;

    ss_dassert(querybuf->next == NULL); // The buffer must be contiguous.

    /* packet_type is a problem as it is MySQL specific */
    packet_type = determine_packet_type(querybuf, &non_empty_packet);
    qtype = determine_query_type(querybuf, packet_type, non_empty_packet);

    if (non_empty_packet)
    {
        handle_multi_temp_and_load(rses, querybuf, packet_type, &qtype);

        if (MXS_LOG_PRIORITY_IS_ENABLED(LOG_INFO))
        {
            log_transaction_status(rses, querybuf, qtype);
        }
        /**
         * Find out where to route the query. Result may not be clear; it is
         * possible to have a hint for routing to a named server which can
         * be either slave or master.
         * If query would otherwise be routed to slave then the hint determines
         * actual target server if it exists.
         *
         * route_target is a bitfield and may include :
         * TARGET_ALL
         * - route to all connected backend servers
         * TARGET_SLAVE[|TARGET_NAMED_SERVER|TARGET_RLAG_MAX]
         * - route primarily according to hints, then to slave and if those
         *   failed, eventually to master
         * TARGET_MASTER[|TARGET_NAMED_SERVER|TARGET_RLAG_MAX]
         * - route primarily according to the hints and if they failed,
         *   eventually to master
         */
        route_target = get_route_target(rses, qtype, querybuf->hint);
    }
    else
    {
        /** Empty packet signals end of LOAD DATA LOCAL INFILE, send it to master*/
        route_target = TARGET_MASTER;
        rses->load_data_state = LOAD_DATA_END;
        MXS_INFO("> LOAD DATA LOCAL INFILE finished: %lu bytes sent.",
                 rses->rses_load_data_sent + gwbuf_length(querybuf));
    }

    SRWBackend target;

    if (TARGET_IS_ALL(route_target))
    {
        succp = handle_target_is_all(route_target, inst, rses, querybuf, packet_type, qtype);
    }
    else
    {
        bool store_stmt = false;
        /**
         * There is a hint which either names the target backend or
         * hint which sets maximum allowed replication lag for the
         * backend.
         */
        if (TARGET_IS_NAMED_SERVER(route_target) ||
            TARGET_IS_RLAG_MAX(route_target))
        {
            succp = handle_hinted_target(rses, querybuf, route_target, target);
        }
        else if (TARGET_IS_SLAVE(route_target))
        {
            succp = handle_slave_is_target(inst, rses, target);
            store_stmt = rses->rses_config.retry_failed_reads;
        }
        else if (TARGET_IS_MASTER(route_target))
        {
            succp = handle_master_is_target(inst, rses, target);

            if (!rses->rses_config.strict_multi_stmt &&
                rses->target_node == rses->current_master)
            {
                /** Reset the forced node as we're in relaxed multi-statement mode */
                rses->target_node.reset();
            }
        }

        if (target && succp) /*< Have DCB of the target backend */
        {
            ss_dassert(!store_stmt || TARGET_IS_SLAVE(route_target));
            handle_got_target(inst, rses, querybuf, target, store_stmt);
        }
    }

    if (succp && inst->rwsplit_config.connection_keepalive &&
        (TARGET_IS_SLAVE(route_target) || TARGET_IS_MASTER(route_target)))
    {
        handle_connection_keepalive(inst, rses, target);
    }

    return succp;
} /* route_single_stmt */

/**
 * Execute in backends used by current router session.
 * Save session variable commands to router session property
 * struct. Thus, they can be replayed in backends which are
 * started and joined later.
 *
 * Suppress redundant OK packets sent by backends.
 *
 * The first OK packet is replied to the client.
 *
 * @param router_cli_ses    Client's router session pointer
 * @param querybuf      GWBUF including the query to be routed
 * @param inst          Router instance
 * @param packet_type       Type of MySQL packet
 * @param qtype         Query type from query_classifier
 *
 * @return True if at least one backend is used and routing succeed to all
 * backends being used, otherwise false.
 *
 */
bool route_session_write(ROUTER_CLIENT_SES *rses, GWBUF *querybuf, uint8_t command)
{
    /** The SessionCommand takes ownership of the buffer */
    uint64_t id = rses->sescmd_count++;
    mxs::SSessionCommand sescmd(new mxs::SessionCommand(querybuf, id));
    bool expecting_response = command_will_respond(command);
    int nsucc = 0;
    uint64_t lowest_pos = id;

    MXS_INFO("Session write, routing to all servers.");

    for (SRWBackendList::iterator it = rses->backends.begin();
         it != rses->backends.end(); it++)
    {
        SRWBackend& bref = *it;

        if (bref->in_use())
        {
            bref->append_session_command(sescmd);

            uint64_t current_pos = bref->next_session_command()->get_position();

            if (current_pos < lowest_pos)
            {
                lowest_pos = current_pos;
            }

            if (bref->execute_session_command())
            {
                nsucc += 1;

                if (expecting_response)
                {
                    rses->expected_responses++;
                }

                MXS_INFO("Route query to %s \t[%s]:%d",
                         SERVER_IS_MASTER(bref->server()) ? "master" : "slave",
                         bref->server()->name, bref->server()->port);
            }
            else
            {
                MXS_ERROR("Failed to execute session command in [%s]:%d",
                          bref->server()->name, bref->server()->port);
            }
        }
    }

    if (rses->rses_config.max_sescmd_history > 0 &&
        rses->sescmd_count >= rses->rses_config.max_sescmd_history)
    {
        MXS_WARNING("Router session exceeded session command history limit. "
                    "Slave recovery is disabled and only slave servers with "
                    "consistent session state are used "
                    "for the duration of the session.");
        rses->rses_config.disable_sescmd_history = true;
        rses->rses_config.max_sescmd_history = 0;
        rses->sescmd_list.clear();
    }

    if (rses->rses_config.disable_sescmd_history)
    {
        /** Prune stored responses */
        ResponseMap::iterator it = rses->sescmd_responses.lower_bound(lowest_pos);

        if (it != rses->sescmd_responses.end())
        {
            rses->sescmd_responses.erase(rses->sescmd_responses.begin(), it);
        }
    }
    else
    {
        rses->sescmd_list.push_back(sescmd);
    }

    if (nsucc)
    {
        rses->sent_sescmd = id;
    }

    return nsucc;
}

/**
 * Provide the router with a reference to a suitable backend
 *
 * @param rses     Pointer to router client session
 * @param btype    Backend type
 * @param name     Name of the backend which is primarily searched. May be NULL.
 * @param max_rlag Maximum replication lag
 * @param target   The target backend
 *
 * @return True if a backend was found
 */
bool get_target_backend(ROUTER_CLIENT_SES *rses, backend_type_t btype,
                        char *name, int max_rlag, SRWBackend& target)
{
    CHK_CLIENT_RSES(rses);

    /** Check whether using rses->target_node as target SLAVE */
    if (rses->target_node && session_trx_is_read_only(rses->client_dcb->session))
    {
        MXS_DEBUG("In READ ONLY transaction, using server '%s'",
                  rses->target_node->server()->unique_name);
        target = rses->target_node;
        return true;
    }

    bool succp = false;

    /** get root master from available servers */
    SRWBackend master_bref = get_root_master_bref(rses);

    if (name) /*< Choose backend by name from a hint */
    {
        ss_dassert(btype != BE_MASTER); /*< Master dominates and no name should be passed with it */

        for (SRWBackendList::iterator it = rses->backends.begin();
             it != rses->backends.end(); it++)
        {
            SRWBackend& bref = *it;

            /** The server must be a valid slave, relay server, or master */

            if (bref->in_use() && bref->is_active() &&
                (strcasecmp(name, bref->server()->unique_name) == 0) &&
                (SERVER_IS_SLAVE(bref->server()) ||
                 SERVER_IS_RELAY_SERVER(bref->server()) ||
                 SERVER_IS_MASTER(bref->server())))
            {
                target = bref;
                return true;
            }
        }

        /** No server found, use a normal slave for it */
        btype = BE_SLAVE;
    }

    if (btype == BE_SLAVE)
    {
        SRWBackend candidate_bref;

        for (SRWBackendList::iterator it = rses->backends.begin();
             it != rses->backends.end(); it++)
        {
            SRWBackend& bref = *it;

            /**
             * Unused backend or backend which is not master nor
             * slave can't be used
             */
            if (!bref->in_use() || !bref->is_active() ||
                (!SERVER_IS_MASTER(bref->server()) && !SERVER_IS_SLAVE(bref->server())))
            {
                continue;
            }
            /**
             * If there are no candidates yet accept both master or
             * slave.
             */
            else if (!candidate_bref)
            {
                /**
                 * Ensure that master has not changed during
                 * session and abort if it has.
                 */
                if (SERVER_IS_MASTER(bref->server()) && bref == rses->current_master)
                {
                    /** found master */
                    candidate_bref = bref;
                    succp = true;
                }
                /**
                 * Ensure that max replication lag is not set
                 * or that candidate's lag doesn't exceed the
                 * maximum allowed replication lag.
                 */
                else if (max_rlag == MAX_RLAG_UNDEFINED ||
                         (bref->server()->rlag != MAX_RLAG_NOT_AVAILABLE &&
                          bref->server()->rlag <= max_rlag))
                {
                    /** found slave */
                    candidate_bref = bref;
                    succp = true;
                }
            }
            /**
             * If candidate is master, any slave which doesn't break
             * replication lag limits replaces it.
             */
            else if (SERVER_IS_MASTER(candidate_bref->server()) &&
                     SERVER_IS_SLAVE(bref->server()) &&
                     (max_rlag == MAX_RLAG_UNDEFINED ||
                      (bref->server()->rlag != MAX_RLAG_NOT_AVAILABLE &&
                       bref->server()->rlag <= max_rlag)) &&
                     !rses->rses_config.master_accept_reads)
            {
                /** found slave */
                candidate_bref = bref;
                succp = true;
            }
            /**
             * When candidate exists, compare it against the current
             * backend and update assign it to new candidate if
             * necessary.
             */
            else if (SERVER_IS_SLAVE(bref->server()) ||
                     (rses->rses_config.master_accept_reads &&
                      SERVER_IS_MASTER(bref->server())))
            {
                if (max_rlag == MAX_RLAG_UNDEFINED ||
                    (bref->server()->rlag != MAX_RLAG_NOT_AVAILABLE &&
                     bref->server()->rlag <= max_rlag))
                {
                    candidate_bref = check_candidate_bref(candidate_bref, bref,
                                                          rses->rses_config.slave_selection_criteria);
                }
                else
                {
                    MXS_INFO("Server [%s]:%d is too much behind the master "
                             "(%d seconds) and can't be chosen",
                             bref->server()->name, bref->server()->port,
                             bref->server()->rlag);
                }
            }
        } /*<  for */

        /** Assign selected DCB's pointer value */
        if (candidate_bref)
        {
            target = candidate_bref;
        }

    }
    /**
     * If target was originally master only then the execution jumps
     * directly here.
     */
    else if (btype == BE_MASTER)
    {
        if (master_bref && master_bref->is_active())
        {
            /** It is possible for the server status to change at any point in time
             * so copying it locally will make possible error messages
             * easier to understand */
            SERVER server;
            server.status = master_bref->server()->status;

            if (master_bref->in_use())
            {
                if (SERVER_IS_MASTER(&server))
                {
                    target = master_bref;
                    succp = true;
                }
                else
                {
                    MXS_ERROR("Server '%s' should be master but is %s instead "
                              "and can't be chosen as the master.",
                              master_bref->server()->unique_name,
                              STRSRVSTATUS(&server));
                    succp = false;
                }
            }
            else
            {
                MXS_ERROR("Server '%s' is not in use and can't be chosen as the master.",
                          master_bref->server()->unique_name);
                succp = false;
            }
        }
    }

    return succp;
}

/**
 * Examine the query type, transaction state and routing hints. Find out the
 * target for query routing.
 *
 *  @param qtype      Type of query
 *  @param trx_active Is transacation active or not
 *  @param hint       Pointer to list of hints attached to the query buffer
 *
 *  @return bitfield including the routing target, or the target server name
 *          if the query would otherwise be routed to slave.
 */
route_target_t get_route_target(ROUTER_CLIENT_SES *rses,
                                uint32_t qtype, HINT *hint)
{
    bool trx_active = session_trx_is_active(rses->client_dcb->session);
    bool load_active = rses->load_data_state != LOAD_DATA_INACTIVE;
    mxs_target_t use_sql_variables_in = rses->rses_config.use_sql_variables_in;
    int target = TARGET_UNDEFINED;

    if (rses->target_node && rses->target_node == rses->current_master)
    {
        target = TARGET_MASTER;
    }
    /**
     * These queries are not affected by hints
     */
    else if (!load_active &&
             (qc_query_is_type(qtype, QUERY_TYPE_SESSION_WRITE) ||
              /** Configured to allow writing user variables to all nodes */
              (use_sql_variables_in == TYPE_ALL &&
               qc_query_is_type(qtype, QUERY_TYPE_USERVAR_WRITE)) ||
              qc_query_is_type(qtype, QUERY_TYPE_GSYSVAR_WRITE) ||
              /** enable or disable autocommit are always routed to all */
              qc_query_is_type(qtype, QUERY_TYPE_ENABLE_AUTOCOMMIT) ||
              qc_query_is_type(qtype, QUERY_TYPE_DISABLE_AUTOCOMMIT)))
    {
        /**
         * This is problematic query because it would be routed to all
         * backends but since this is SELECT that is not possible:
         * 1. response set is not handled correctly in clientReply and
         * 2. multiple results can degrade performance.
         *
         * Prepared statements are an exception to this since they do not
         * actually do anything but only prepare the statement to be used.
         * They can be safely routed to all backends since the execution
         * is done later.
         *
         * With prepared statement caching the task of routing
         * the execution of the prepared statements to the right server would be
         * an easy one. Currently this is not supported.
         */
        if (qc_query_is_type(qtype, QUERY_TYPE_READ) &&
            !(qc_query_is_type(qtype, QUERY_TYPE_PREPARE_STMT) ||
              qc_query_is_type(qtype, QUERY_TYPE_PREPARE_NAMED_STMT)))
        {
            MXS_WARNING("The query can't be routed to all "
                        "backend servers because it includes SELECT and "
                        "SQL variable modifications which is not supported. "
                        "Set use_sql_variables_in=master or split the "
                        "query to two, where SQL variable modifications "
                        "are done in the first and the SELECT in the "
                        "second one.");

            target = TARGET_MASTER;
        }
        target |= TARGET_ALL;
    }
    /**
     * Hints may affect on routing of the following queries
     */
    else if (!trx_active && !load_active &&
             !qc_query_is_type(qtype, QUERY_TYPE_MASTER_READ) &&
             !qc_query_is_type(qtype, QUERY_TYPE_WRITE) &&
             !qc_query_is_type(qtype, QUERY_TYPE_PREPARE_STMT) &&
             !qc_query_is_type(qtype, QUERY_TYPE_PREPARE_NAMED_STMT) &&
             (qc_query_is_type(qtype, QUERY_TYPE_READ) ||
              qc_query_is_type(qtype, QUERY_TYPE_SHOW_TABLES) ||
              qc_query_is_type(qtype, QUERY_TYPE_USERVAR_READ) ||
              qc_query_is_type(qtype, QUERY_TYPE_SYSVAR_READ) ||
              qc_query_is_type(qtype, QUERY_TYPE_GSYSVAR_READ)))
    {
        if (qc_query_is_type(qtype, QUERY_TYPE_USERVAR_READ))
        {
            if (use_sql_variables_in == TYPE_ALL)
            {
                target = TARGET_SLAVE;
            }
        }
        else if (qc_query_is_type(qtype, QUERY_TYPE_READ) || // Normal read
                 qc_query_is_type(qtype, QUERY_TYPE_SHOW_TABLES) || // SHOW TABLES
                 qc_query_is_type(qtype, QUERY_TYPE_SYSVAR_READ) || // System variable
                 qc_query_is_type(qtype, QUERY_TYPE_GSYSVAR_READ)) // Global system variable
        {
            target = TARGET_SLAVE;
        }

        /** If nothing matches then choose the master */
        if ((target & (TARGET_ALL | TARGET_SLAVE | TARGET_MASTER)) == 0)
        {
            target = TARGET_MASTER;
        }
    }
    else if (session_trx_is_read_only(rses->client_dcb->session))
    {
        /* Force TARGET_SLAVE for READ ONLY tranaction (active or ending) */
        target = TARGET_SLAVE;
    }
    else
    {
        ss_dassert(trx_active || load_active ||
                   (qc_query_is_type(qtype, QUERY_TYPE_WRITE) ||
                    qc_query_is_type(qtype, QUERY_TYPE_MASTER_READ) ||
                    qc_query_is_type(qtype, QUERY_TYPE_SESSION_WRITE) ||
                    (qc_query_is_type(qtype, QUERY_TYPE_USERVAR_READ) &&
                     use_sql_variables_in == TYPE_MASTER) ||
                    (qc_query_is_type(qtype, QUERY_TYPE_SYSVAR_READ) &&
                     use_sql_variables_in == TYPE_MASTER) ||
                    (qc_query_is_type(qtype, QUERY_TYPE_GSYSVAR_READ) &&
                     use_sql_variables_in == TYPE_MASTER) ||
                    (qc_query_is_type(qtype, QUERY_TYPE_GSYSVAR_WRITE) &&
                     use_sql_variables_in == TYPE_MASTER) ||
                    (qc_query_is_type(qtype, QUERY_TYPE_USERVAR_WRITE) &&
                     use_sql_variables_in == TYPE_MASTER) ||
                    qc_query_is_type(qtype, QUERY_TYPE_BEGIN_TRX) ||
                    qc_query_is_type(qtype, QUERY_TYPE_ENABLE_AUTOCOMMIT) ||
                    qc_query_is_type(qtype, QUERY_TYPE_DISABLE_AUTOCOMMIT) ||
                    qc_query_is_type(qtype, QUERY_TYPE_ROLLBACK) ||
                    qc_query_is_type(qtype, QUERY_TYPE_COMMIT) ||
                    qc_query_is_type(qtype, QUERY_TYPE_EXEC_STMT) ||
                    qc_query_is_type(qtype, QUERY_TYPE_CREATE_TMP_TABLE) ||
                    qc_query_is_type(qtype, QUERY_TYPE_READ_TMP_TABLE) ||
                    qc_query_is_type(qtype, QUERY_TYPE_UNKNOWN)) ||
                   qc_query_is_type(qtype, QUERY_TYPE_EXEC_STMT) ||
                   qc_query_is_type(qtype, QUERY_TYPE_PREPARE_STMT) ||
                   qc_query_is_type(qtype, QUERY_TYPE_PREPARE_NAMED_STMT));

        target = TARGET_MASTER;
    }

    /** process routing hints */
    while (hint != NULL)
    {
        if (hint->type == HINT_ROUTE_TO_MASTER)
        {
            target = TARGET_MASTER; /*< override */
            MXS_DEBUG("%lu [get_route_target] Hint: route to master.",
                      pthread_self());
            break;
        }
        else if (hint->type == HINT_ROUTE_TO_NAMED_SERVER)
        {
            /**
             * Searching for a named server. If it can't be
             * found, the oroginal target is chosen.
             */
            target |= TARGET_NAMED_SERVER;
            MXS_DEBUG("%lu [get_route_target] Hint: route to "
                      "named server : ",
                      pthread_self());
        }
        else if (hint->type == HINT_ROUTE_TO_UPTODATE_SERVER)
        {
            /** not implemented */
        }
        else if (hint->type == HINT_ROUTE_TO_ALL)
        {
            /** not implemented */
        }
        else if (hint->type == HINT_PARAMETER)
        {
            if (strncasecmp((char *)hint->data, "max_slave_replication_lag",
                            strlen("max_slave_replication_lag")) == 0)
            {
                target |= TARGET_RLAG_MAX;
            }
            else
            {
                MXS_ERROR("Unknown hint parameter "
                          "'%s' when 'max_slave_replication_lag' "
                          "was expected.",
                          (char *)hint->data);
            }
        }
        else if (hint->type == HINT_ROUTE_TO_SLAVE)
        {
            target = TARGET_SLAVE;
            MXS_DEBUG("%lu [get_route_target] Hint: route to "
                      "slave.",
                      pthread_self());
        }
        hint = hint->next;
    } /*< while (hint != NULL) */

    return (route_target_t)target;
}

/**
 * @brief Handle multi statement queries and load statements
 *
 * One of the possible types of handling required when a request is routed
 *
 *  @param ses          Router session
 *  @param querybuf     Buffer containing query to be routed
 *  @param packet_type  Type of packet (database specific)
 *  @param qtype        Query type
 */
void
handle_multi_temp_and_load(ROUTER_CLIENT_SES *rses, GWBUF *querybuf,
                           uint8_t packet_type, uint32_t *qtype)
{
    /** Check for multi-statement queries. If no master server is available
     * and a multi-statement is issued, an error is returned to the client
     * when the query is routed.
     *
     * If we do not have a master node, assigning the forced node is not
     * effective since we don't have a node to force queries to. In this
     * situation, assigning QUERY_TYPE_WRITE for the query will trigger
     * the error processing. */
    if ((rses->target_node == NULL || rses->target_node != rses->current_master) &&
        check_for_multi_stmt(querybuf, rses->client_dcb->protocol, packet_type))
    {
        if (rses->current_master)
        {
            rses->target_node = rses->current_master;
            MXS_INFO("Multi-statement query, routing all future queries to master.");
        }
        else
        {
            *qtype |= QUERY_TYPE_WRITE;
        }
    }

    /*
     * Make checks prior to calling temp tables functions
     */

    if (rses == NULL || querybuf == NULL ||
        rses->client_dcb == NULL || rses->client_dcb->data == NULL)
    {
        if (rses == NULL || querybuf == NULL)
        {
            MXS_ERROR("[%s] Error: NULL variables for temp table checks: %p %p", __FUNCTION__,
                      rses, querybuf);
        }

        if (rses->client_dcb == NULL)
        {
            MXS_ERROR("[%s] Error: Client DCB is NULL.", __FUNCTION__);
        }

        if (rses->client_dcb->data == NULL)
        {
            MXS_ERROR("[%s] Error: User data in master server DBC is NULL.",
                      __FUNCTION__);
        }
    }

    else
    {
        /**
         * Check if the query has anything to do with temporary tables.
         */
        if (rses->have_tmp_tables)
        {
            check_drop_tmp_table(rses, querybuf);
            if (is_packet_a_query(packet_type) && is_read_tmp_table(rses, querybuf, *qtype))
            {
                *qtype |= QUERY_TYPE_MASTER_READ;
            }
        }
        check_create_tmp_table(rses, querybuf, *qtype);
    }

    /**
     * Check if this is a LOAD DATA LOCAL INFILE query. If so, send all queries
     * to the master until the last, empty packet arrives.
     */
    if (rses->load_data_state == LOAD_DATA_ACTIVE)
    {
        rses->rses_load_data_sent += gwbuf_length(querybuf);
    }
    else if (is_packet_a_query(packet_type))
    {
        qc_query_op_t queryop = qc_get_operation(querybuf);
        if (queryop == QUERY_OP_LOAD)
        {
            rses->load_data_state = LOAD_DATA_START;
            rses->rses_load_data_sent = 0;
        }
    }
}

/**
 * @brief Handle hinted target query
 *
 * One of the possible types of handling required when a request is routed
 *
 *  @param ses          Router session
 *  @param querybuf     Buffer containing query to be routed
 *  @param route_target Target for the query
 *  @param target_dcb   DCB for the target server
 *
 *  @return bool - true if succeeded, false otherwise
 */
bool handle_hinted_target(ROUTER_CLIENT_SES *rses, GWBUF *querybuf,
                          route_target_t route_target, SRWBackend& target)
{
    char *named_server = NULL;
    int rlag_max = MAX_RLAG_UNDEFINED;
    bool succp;

    HINT* hint = querybuf->hint;

    while (hint != NULL)
    {
        if (hint->type == HINT_ROUTE_TO_NAMED_SERVER)
        {
            /**
             * Set the name of searched
             * backend server.
             */
            named_server = (char*)hint->data;
            MXS_INFO("Hint: route to server '%s'", named_server);
        }
        else if (hint->type == HINT_PARAMETER &&
                 (strncasecmp((char *)hint->data, "max_slave_replication_lag",
                              strlen("max_slave_replication_lag")) == 0))
        {
            int val = (int)strtol((char *)hint->value, (char **)NULL, 10);

            if (val != 0 || errno == 0)
            {
                /** Set max. acceptable replication lag value for backend srv */
                rlag_max = val;
                MXS_INFO("Hint: max_slave_replication_lag=%d", rlag_max);
            }
        }
        hint = hint->next;
    } /*< while */

    if (rlag_max == MAX_RLAG_UNDEFINED) /*< no rlag max hint, use config */
    {
        rlag_max = rses_get_max_replication_lag(rses);
    }

    /** target may be master or slave */
    backend_type_t btype = route_target & TARGET_SLAVE ? BE_SLAVE : BE_MASTER;

    /**
     * Search backend server by name or replication lag.
     * If it fails, then try to find valid slave or master.
     */
    succp = get_target_backend(rses, btype, named_server, rlag_max, target);

    if (!succp)
    {
        if (TARGET_IS_NAMED_SERVER(route_target))
        {
            MXS_INFO("Was supposed to route to named server "
                     "%s but couldn't find the server in a "
                     "suitable state.", named_server);
        }
        else if (TARGET_IS_RLAG_MAX(route_target))
        {
            MXS_INFO("Was supposed to route to server with "
                     "replication lag at most %d but couldn't "
                     "find such a slave.", rlag_max);
        }
    }
    return succp;
}

/**
 * @brief Handle slave is the target
 *
 * One of the possible types of handling required when a request is routed
 *
 *  @param inst         Router instance
 *  @param ses          Router session
 *  @param target_dcb   DCB for the target server
 *
 *  @return bool - true if succeeded, false otherwise
 */
bool handle_slave_is_target(ROUTER_INSTANCE *inst, ROUTER_CLIENT_SES *rses,
                            SRWBackend& target)
{
    int rlag_max = rses_get_max_replication_lag(rses);

    /**
     * Search suitable backend server, get DCB in target_dcb
     */
    if (get_target_backend(rses, BE_SLAVE, NULL, rlag_max, target))
    {
        atomic_add_uint64(&inst->stats.n_slave, 1);
        return true;
    }
    else
    {
        MXS_INFO("Was supposed to route to slave but finding suitable one failed.");
        return false;
    }
}

/**
 * @brief Log master write failure
 *
 * @param rses Router session
 */
static void log_master_routing_failure(ROUTER_CLIENT_SES *rses, bool found,
                                       SRWBackend& old_master, SRWBackend& curr_master)
{
    char errmsg[MAX_SERVER_ADDRESS_LEN * 2 + 100]; // Extra space for error message

    if (!found)
    {
        sprintf(errmsg, "Could not find a valid master connection");
    }
    else if (old_master && curr_master)
    {
        /** We found a master but it's not the same connection */
        ss_dassert(old_master != curr_master);
        if (old_master->server() != curr_master->server())
        {
            sprintf(errmsg, "Master server changed from '%s' to '%s'",
                    old_master->server()->unique_name,
                    curr_master->server()->unique_name);
        }
        else
        {
            ss_dassert(false); // Currently we don't reconnect to the master
            sprintf(errmsg, "Connection to master '%s' was recreated",
                    curr_master->server()->unique_name);
        }
    }
    else if (old_master)
    {
        /** We have an original master connection but we couldn't find it */
        sprintf(errmsg, "The connection to master server '%s' is not available",
                old_master->server()->unique_name);
    }
    else
    {
        /** We never had a master connection, the session must be in read-only mode */
        if (rses->rses_config.master_failure_mode != RW_FAIL_INSTANTLY)
        {
            sprintf(errmsg, "Session is in read-only mode because it was created "
                    "when no master was available");
        }
        else
        {
            ss_dassert(false); // A session should always have a master reference
            sprintf(errmsg, "Was supposed to route to master but couldn't "
                    "find master in a suitable state");
        }
    }

    MXS_WARNING("[%s] Write query received from %s@%s. %s. Closing client connection.",
                rses->router->service->name, rses->client_dcb->user,
                rses->client_dcb->remote, errmsg);
}

/**
 * @brief Handle master is the target
 *
 * One of the possible types of handling required when a request is routed
 *
 *  @param inst         Router instance
 *  @param ses          Router session
 *  @param target_dcb   DCB for the target server
 *
 *  @return bool - true if succeeded, false otherwise
 */
bool handle_master_is_target(ROUTER_INSTANCE *inst, ROUTER_CLIENT_SES *rses,
                             SRWBackend& target)
{
    DCB *curr_master_dcb = NULL;
    bool succp = get_target_backend(rses, BE_MASTER, NULL, MAX_RLAG_UNDEFINED, target);

    if (succp && target == rses->current_master)
    {
        atomic_add_uint64(&inst->stats.n_master, 1);
    }
    else
    {
        /** The original master is not available, we can't route the write */
        if (rses->rses_config.master_failure_mode == RW_ERROR_ON_WRITE)
        {
            succp = send_readonly_error(rses->client_dcb);

            if (rses->current_master && rses->current_master->in_use())
            {
                rses->current_master->close();
            }
        }
        else
        {
            log_master_routing_failure(rses, succp, rses->current_master, target);
            succp = false;
        }
    }

    return succp;
}

static inline bool query_creates_reply(mysql_server_cmd_t cmd)
{
    return cmd != MYSQL_COM_QUIT &&
           cmd != MYSQL_COM_STMT_SEND_LONG_DATA &&
           cmd != MYSQL_COM_STMT_CLOSE;
}

/**
 * @brief Handle got a target
 *
 * One of the possible types of handling required when a request is routed
 *
 *  @param inst         Router instance
 *  @param ses          Router session
 *  @param querybuf     Buffer containing query to be routed
 *  @param target_dcb   DCB for the target server
 *
 *  @return bool - true if succeeded, false otherwise
 */
bool
handle_got_target(ROUTER_INSTANCE *inst, ROUTER_CLIENT_SES *rses,
                  GWBUF *querybuf, SRWBackend& target, bool store)
{
    /**
     * If the transaction is READ ONLY set forced_node to bref
     * That SLAVE backend will be used until COMMIT is seen
     */
    if (!rses->target_node &&
        session_trx_is_read_only(rses->client_dcb->session))
    {
        rses->target_node = target;
        MXS_DEBUG("Setting forced_node SLAVE to %s within an opened READ ONLY transaction",
                  target->server()->unique_name);
    }

    MXS_INFO("Route query to %s \t[%s]:%d <",
             (SERVER_IS_MASTER(target->server()) ? "master" : "slave"),
             target->server()->name, target->server()->port);

    /** The session command cursor must not be active */
    ss_dassert(target->session_command_count() == 0);

    /** We only want the complete response to the preparation */
    if (MYSQL_GET_COMMAND(GWBUF_DATA(querybuf)) == MYSQL_COM_STMT_PREPARE)
    {
        gwbuf_set_type(querybuf, GWBUF_TYPE_COLLECT_RESULT);
    }

    mxs::Backend::response_type response = mxs::Backend::NO_RESPONSE;
    mysql_server_cmd_t cmd = mxs_mysql_current_command(rses->client_dcb->session);

    if (rses->load_data_state != LOAD_DATA_ACTIVE &&
        query_creates_reply(cmd))
    {
        response = mxs::Backend::EXPECT_RESPONSE;
    }

    if (target->write(gwbuf_clone(querybuf), response))
    {
        if (store && !session_store_stmt(rses->client_dcb->session, querybuf, target->server()))
        {
            MXS_ERROR("Failed to store current statement, it won't be retried if it fails.");
        }

        atomic_add_uint64(&inst->stats.n_queries, 1);

        if (response == mxs::Backend::EXPECT_RESPONSE)
        {
            /** The server will reply to this command */
            ss_dassert(target->get_reply_state() == REPLY_STATE_DONE);

            LOG_RS(target, REPLY_STATE_START);
            target->set_reply_state(REPLY_STATE_START);
            rses->expected_responses++;

            if (rses->load_data_state == LOAD_DATA_START)
            {
                /** The first packet contains the actual query and the server
                 * will respond to it */
                rses->load_data_state = LOAD_DATA_ACTIVE;
            }
            else if (rses->load_data_state == LOAD_DATA_END)
            {
                /** The final packet in a LOAD DATA LOCAL INFILE is an empty packet
                 * to which the server responds with an OK or an ERR packet */
                ss_dassert(gwbuf_length(querybuf) == 4);
                rses->load_data_state = LOAD_DATA_INACTIVE;
            }
        }

        /**
         * If a READ ONLY transaction is ending set forced_node to NULL
         */
        if (rses->target_node &&
            session_trx_is_read_only(rses->client_dcb->session) &&
            session_trx_is_ending(rses->client_dcb->session))
        {
            MXS_DEBUG("An opened READ ONLY transaction ends: forced_node is set to NULL");
            rses->target_node.reset();
        }
        return true;
    }
    else
    {
        MXS_ERROR("Routing query failed.");
        return false;
    }
}

/********************************
 * This routine returns the root master server from MySQL replication tree
 * Get the root Master rule:
 *
 * find server with the lowest replication depth level
 * and the SERVER_MASTER bitval
 * Servers are checked even if they are in 'maintenance'
 *
 * @param   rses pointer to router session
 * @return  pointer to backend reference of the root master or NULL
 *
 */
static SRWBackend get_root_master_bref(ROUTER_CLIENT_SES *rses)
{
    SRWBackend candidate;
    SERVER master = {};

    for (SRWBackendList::iterator it = rses->backends.begin();
         it != rses->backends.end(); it++)
    {
        SRWBackend& bref = *it;
        if (bref->in_use())
        {
            if (bref == rses->current_master)
            {
                /** Store master state for better error reporting */
                master.status = bref->server()->status;
            }

            if (SERVER_IS_MASTER(bref->server()))
            {
                if (!candidate ||
                    (bref->server()->depth < candidate->server()->depth))
                {
                    candidate = bref;
                }
            }
        }
    }

    if (!candidate && rses->rses_config.master_failure_mode == RW_FAIL_INSTANTLY &&
        rses->current_master && rses->current_master->in_use())
    {
        MXS_ERROR("Could not find master among the backend servers. "
                  "Previous master's state : %s", STRSRVSTATUS(&master));
    }

    return candidate;
}
