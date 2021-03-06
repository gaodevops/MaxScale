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

/**
 * @file namedserverfilter.cc - a very simple regular expression based filter
 * that routes to a named server or server type if a regular expression match
 * is found.
 * @verbatim
 *
 * A simple regular expression based query routing filter.
 * Two parameters should be defined in the filter configuration
 *      match=<regular expression>
 *      server=<server to route statement to>
 * Two optional parameters
 *      source=<source address to limit filter>
 *      user=<username to limit filter>
 *
 * @endverbatim
 */

#define MXS_MODULE_NAME "namedserverfilter"

#include "namedserverfilter.hh"

#include <stdio.h>
#include <string>
#include <string.h>
#include <vector>

#include <maxscale/alloc.h>
#include <maxscale/hint.h>
#include <maxscale/log_manager.h>
#include <maxscale/modinfo.h>
#include <maxscale/modutil.h>
#include <maxscale/server.h>
#include <maxscale/utils.h>

using std::string;

static void generate_param_names(int pairs);

/* These arrays contain the allowed indexed config parameter names. match01,
 * target01, match02, target02 ... */
static StringArray param_names_match_indexed;
static StringArray param_names_target_indexed;

static const MXS_ENUM_VALUE option_values[] =
{
    {"ignorecase", PCRE2_CASELESS},
    {"case", 0},
    {"extended", PCRE2_EXTENDED}, // Ignore white space and # comments
    {NULL}
};

static const char MATCH_STR[] = "match";
static const char SERVER_STR[] = "server";
static const char TARGET_STR[] = "target";

RegexHintFilter::RegexHintFilter(string user, SourceHost* source,
                                 const MappingArray& mapping, int ovector_size)
    :   m_user(user),
        m_source(source),
        m_mapping(mapping),
        m_ovector_size(ovector_size),
        m_total_diverted(0),
        m_total_undiverted(0)
{}

RegexHintFilter::~RegexHintFilter()
{
    delete m_source;
    for (unsigned int i = 0; i < m_mapping.size(); i++)
    {
        pcre2_code_free(m_mapping.at(i).m_regex);
    }
}

RegexHintFSession::RegexHintFSession(MXS_SESSION* session,
                                     RegexHintFilter& fil_inst,
                                     bool active, pcre2_match_data* md)
    : maxscale::FilterSession::FilterSession(session),
      m_fil_inst(fil_inst),
      m_n_diverted(0),
      m_n_undiverted(0),
      m_active(active),
      m_match_data(md)
{}

RegexHintFSession::~RegexHintFSession()
{
    pcre2_match_data_free(m_match_data);
}

/**
 * If the regular expression configured in the match parameter of the
 * filter definition matches the SQL text then add the hint
 * "Route to named server" with the name defined in the regex-server mapping
 *
 * @param queue     The query data
 * @return 1 on success, 0 on failure
 */
int RegexHintFSession::routeQuery(GWBUF* queue)
{
    char* sql = NULL;
    int sql_len = 0;

    if (modutil_is_SQL(queue) && m_active)
    {
        if (modutil_extract_SQL(queue, &sql, &sql_len))
        {
            const RegexToServers* reg_serv =
                m_fil_inst.find_servers(sql, sql_len, m_match_data);

            if (reg_serv)
            {
                /* Add the servers in the list to the buffer routing hints */
                for (unsigned int i = 0; i < reg_serv->m_targets.size(); i++)
                {
                    queue->hint =
                        hint_create_route(queue->hint, reg_serv->m_htype,
                                          ((reg_serv->m_targets)[i]).c_str());
                }
                m_n_diverted++;
                m_fil_inst.m_total_diverted++;
            }
            else
            {
                m_n_undiverted++;
                m_fil_inst.m_total_undiverted++;
            }
        }
    }
    return m_down.routeQuery(queue);
}

/**
 * Associate a new session with this instance of the filter.
 *
 * @param session   The client session to attach to
 * @return a new filter session
 */
RegexHintFSession* RegexHintFilter::newSession(MXS_SESSION* session)
{
    const char* remote = NULL;
    const char* user = NULL;

    pcre2_match_data* md = pcre2_match_data_create(m_ovector_size, NULL);
    bool session_active = true;

    /* Check client IP against 'source' host option */
    if (m_source && m_source->m_address.length() &&
        (remote = session_get_remote(session)) != NULL)
    {
        session_active =
            check_source_host(remote, &(session->client_dcb->ip));
    }

    /* Check client user against 'user' option */
    if (m_user.length() > 0 &&
        ((user = session_get_user(session)) != NULL) &&
        (user != m_user))
    {
        session_active = false;
    }
    return new RegexHintFSession(session, *this, session_active, md);
}

/**
 * Find the first server list with a matching regular expression.
 *
 * @param sql   SQL-query string, not null-terminated
 * @paran sql_len   length of SQL-query
 * @param match_data    result container, from filter session
 * @return a set of servers from the main mapping container
 */
const RegexToServers*
RegexHintFilter::find_servers(char* sql, int sql_len, pcre2_match_data* match_data)
{
    /* Go through the regex array and find a match. */
    for (unsigned int i = 0; i < m_mapping.size(); i++)
    {
        pcre2_code* regex = m_mapping[i].m_regex;
        int result = pcre2_match(regex, (PCRE2_SPTR)sql, sql_len, 0, 0,
                                 match_data, NULL);
        if (result >= 0)
        {
            /* Have a match. No need to check if the regex matches the complete
             * query, since the user can form the regex to enforce this. */
            return &(m_mapping[i]);
        }
        else if (result != PCRE2_ERROR_NOMATCH)
        {
            /* Error during matching */
            if (!m_mapping[i].m_error_printed)
            {
                MXS_PCRE2_PRINT_ERROR(result);
                m_mapping[i].m_error_printed = true;
            }
            return NULL;
        }
    }
    return NULL;
}

/**
 * Capability routine.
 *
 * @return The capabilities of the filter.
 */
uint64_t RegexHintFilter::getCapabilities()
{
    return RCAP_TYPE_NONE;
}

/**
 * Create an instance of the filter
 *
 * @param name  Filter instance name
 * @param options   The options for this filter
 * @param params    The array of name/value pair parameters for the filter
 *
 * @return The new instance or null on error
 */
RegexHintFilter*
RegexHintFilter::create(const char* name, char** options, MXS_CONFIG_PARAMETER* params)
{
    bool error = false;
    SourceHost* source_host = NULL;

    const char* source = config_get_string(params, "source");
    if (*source)
    {
        source_host = set_source_address(source);
        if (!source_host)
        {
            MXS_ERROR("Failure setting 'source' from %s", source);
            error = true;
        }
    }

    int pcre_ops = config_get_enum(params, "options", option_values);

    string match_val_legacy(config_get_string(params, MATCH_STR));
    string server_val_legacy(config_get_string(params, SERVER_STR));
    const bool legacy_mode = (match_val_legacy.length() || server_val_legacy.length());

    if (legacy_mode && (!match_val_legacy.length() || !server_val_legacy.length()))
    {
        MXS_ERROR("Only one of '%s' and '%s' is set. If using legacy mode, set both."
                  "If using indexed parameters, set neither and use '%s01' and '%s01' etc.",
                  MATCH_STR, SERVER_STR, MATCH_STR, TARGET_STR);
        error = true;
    }

    MappingArray mapping;
    uint32_t max_capcount;
    /* Try to form the mapping with indexed parameter names */
    form_regex_server_mapping(params, pcre_ops, &mapping, &max_capcount);

    if (!legacy_mode && !mapping.size())
    {
        MXS_ERROR("Could not parse any indexed '%s'-'%s' pairs.", MATCH_STR, TARGET_STR);
        error = true;
    }
    else if (legacy_mode && mapping.size())
    {
        MXS_ERROR("Found both legacy parameters and indexed parameters. Use only "
                  "one type of parameters.");
        error = true;
    }
    else if (legacy_mode && !mapping.size())
    {
        MXS_WARNING("Use of legacy parameters 'match' and 'server' is deprecated.");
        /* Using legacy mode and no indexed parameters found. Add the legacy parameters
         * to the mapping. */
        if (!regex_compile_and_add(pcre_ops, true, match_val_legacy, server_val_legacy,
                                   &mapping, &max_capcount))
        {
            error = true;
        }
    }

    if (error)
    {
        delete source_host;
        return NULL;
    }
    else
    {
        RegexHintFilter* instance = NULL;
        string user(config_get_string(params, "user"));
        MXS_EXCEPTION_GUARD(instance =
                                new RegexHintFilter(user, source_host, mapping, max_capcount + 1));
        return instance;
    }
}

/**
 * Diagnostics routine
 *
 * Print diagnostics on the filter instance as a whole + session-specific info.
 *
 * @param   dcb     The DCB for diagnostic output
 */
void RegexHintFSession::diagnostics(DCB* dcb)
{

    m_fil_inst.diagnostics(dcb); /* Print overall diagnostics */
    dcb_printf(dcb, "\t\tNo. of queries diverted by filter (session): %d\n",
               m_n_diverted);
    dcb_printf(dcb, "\t\tNo. of queries not diverted by filter (session):     %d\n",
               m_n_undiverted);
}

/**
 * Diagnostics routine
 *
 * Print diagnostics on the filter instance as a whole + session-specific info.
 *
 * @param   dcb     The DCB for diagnostic output
 */
json_t* RegexHintFSession::diagnostics_json() const
{

    json_t* rval = m_fil_inst.diagnostics_json(); /* Print overall diagnostics */

    json_object_set_new(rval, "session_queries_diverted", json_integer(m_n_diverted));
    json_object_set_new(rval, "session_queries_undiverted", json_integer(m_n_undiverted));

    return rval;
}

/**
 * Diagnostics routine
 *
 * Print diagnostics on the filter instance as a whole.
 *
 * @param   dcb     The DCB for diagnostic output
 */
void RegexHintFilter::diagnostics(DCB* dcb)
{
    if (m_mapping.size() > 0)
    {
        dcb_printf(dcb, "\t\tMatches and routes:\n");
    }
    for (unsigned int i = 0; i < m_mapping.size(); i++)
    {
        dcb_printf(dcb, "\t\t\t/%s/ -> ",
                   m_mapping[i].m_match.c_str());
        dcb_printf(dcb, "%s", m_mapping[i].m_targets[0].c_str());
        for (unsigned int j = 1; j < m_mapping[i].m_targets.size(); j++)
        {
            dcb_printf(dcb, ", %s", m_mapping[i].m_targets[j].c_str());
        }
        dcb_printf(dcb, "\n");
    }
    dcb_printf(dcb, "\t\tTotal no. of queries diverted by filter (approx.):     %d\n",
               m_total_diverted);
    dcb_printf(dcb, "\t\tTotal no. of queries not diverted by filter (approx.): %d\n",
               m_total_undiverted);

    if (m_source)
    {
        dcb_printf(dcb,
                   "\t\tReplacement limited to connections from     %s\n",
                   m_source->m_address.c_str());
    }
    if (m_user.length())
    {
        dcb_printf(dcb,
                   "\t\tReplacement limit to user           %s\n",
                   m_user.c_str());
    }
}

/**
 * Diagnostics routine
 *
 * Print diagnostics on the filter instance as a whole.
 *
 * @param   dcb     The DCB for diagnostic output
 */
json_t* RegexHintFilter::diagnostics_json() const
{
    json_t* rval = json_object();

    json_object_set_new(rval, "queries_diverted", json_integer(m_total_diverted));
    json_object_set_new(rval, "queries_undiverted", json_integer(m_total_undiverted));

    if (m_mapping.size() > 0)
    {
        json_t* arr = json_array();

        for (MappingArray::const_iterator it = m_mapping.begin(); it != m_mapping.end(); it++)
        {
            json_t* obj = json_object();
            json_t* targets = json_array();

            for (StringArray::const_iterator it2 = it->m_targets.begin(); it2 != it->m_targets.end(); it2++)
            {
                json_array_append_new(targets, json_string(it2->c_str()));
            }

            json_object_set_new(obj, "match", json_string(it->m_match.c_str()));
            json_object_set_new(obj, "targets", targets);
        }

        json_object_set_new(rval, "mappings", arr);
    }

    if (m_source)
    {
        json_object_set_new(rval, "source", json_string(m_source->m_address.c_str()));
    }

    if (m_user.length())
    {
        json_object_set_new(rval, "user", json_string(m_user.c_str()));
    }

    return rval;
}

/**
 * Parse the server list and add the contained servers to the struct's internal
 * list. Server names are verified to be valid servers.
 *
 * @param server_names The list of servers as read from the config file
 * @return How many were found
 */
int RegexToServers::add_servers(string server_names, bool legacy_mode)
{
    if (legacy_mode)
    {
        /* Should have just one server name, already known to be valid */
        m_targets.push_back(server_names);
        return 1;
    }

    /* Have to parse the server list here instead of in config loader, since the list
     * may contain special placeholder strings.
     */
    bool error = false;
    char** names_arr = NULL;
    const int n_names = config_parse_server_list(server_names.c_str(), &names_arr);
    if (n_names > 1)
    {
        /* The string contains a server list, all must be valid servers */
        SERVER **servers;
        int found = server_find_by_unique_names(names_arr, n_names, &servers);
        if (found != n_names)
        {
            error = true;
            for (int i = 0; i < n_names; i++)
            {
                /* servers is valid only if found > 0 */
                if (!found || !servers[i])
                {
                    MXS_ERROR("'%s' is not a valid server name.", names_arr[i]);
                }
            }
        }
        if (found)
        {
            MXS_FREE(servers);
        }
        if (!error)
        {
            for (int i = 0; i < n_names; i++)
            {
                m_targets.push_back(names_arr[i]);
            }
        }
    }
    else if (n_names == 1)
    {
        /* The string is either a server name or a special reserved id */
        if (server_find_by_unique_name(names_arr[0]))
        {
            m_targets.push_back(names_arr[0]);
        }
        else if (strcmp(names_arr[0], "->master") == 0)
        {
            m_targets.push_back(names_arr[0]);
            m_htype = HINT_ROUTE_TO_MASTER;
        }
        else if (strcmp(names_arr[0], "->slave") == 0)
        {
            m_targets.push_back(names_arr[0]);
            m_htype = HINT_ROUTE_TO_SLAVE;
        }
        else if (strcmp(names_arr[0], "->all") == 0)
        {
            m_targets.push_back(names_arr[0]);
            m_htype = HINT_ROUTE_TO_ALL;
        }
        else
        {
            error = true;
        }
    }
    else
    {
        error = true;
    }

    for (int i = 0; i < n_names; i++)
    {
        MXS_FREE(names_arr[i]);
    }
    MXS_FREE(names_arr);
    return error ? 0 : n_names;
}

bool RegexHintFilter::regex_compile_and_add(int pcre_ops, bool legacy_mode,
                                            const string& match, const string& servers,
                                            MappingArray* mapping, uint32_t* max_capcount)
{
    bool success = true;
    int errorcode = -1;
    PCRE2_SIZE error_offset = -1;
    pcre2_code* regex =
        pcre2_compile((PCRE2_SPTR) match.c_str(), match.length(), pcre_ops,
                      &errorcode, &error_offset, NULL);

    if (regex)
    {
        // Try to compile even further for faster matching
        if (pcre2_jit_compile(regex, PCRE2_JIT_COMPLETE) < 0)
        {
            MXS_NOTICE("PCRE2 JIT compilation of pattern '%s' failed, "
                       "falling back to normal compilation.", match.c_str());
        }

        RegexToServers regex_ser(match, regex);
        if (regex_ser.add_servers(servers, legacy_mode) == 0)
        {
            // The servers string didn't seem to contain any servers
            MXS_ERROR("Could not parse servers from string '%s'.", servers.c_str());
            success = false;
        }
        mapping->push_back(regex_ser);

        /* Check what is the required match_data size for this pattern. The
         * largest value is used to form the match data.
         */
        uint32_t capcount = 0;
        int ret_info = pcre2_pattern_info(regex, PCRE2_INFO_CAPTURECOUNT, &capcount);
        if (ret_info != 0)
        {
            MXS_PCRE2_PRINT_ERROR(ret_info);
            success = false;
        }
        else
        {
            if (capcount > *max_capcount)
            {
                *max_capcount = capcount;
            }
        }
    }
    else
    {
        MXS_ERROR("Invalid PCRE2 regular expression '%s' (position '%zu').",
                  match.c_str(), error_offset);
        MXS_PCRE2_PRINT_ERROR(errorcode);
        success = false;
    }
    return success;
}

/**
 * Read all indexed regexes from the supplied configuration, compile them and form the mapping
 *
 * @param params config parameters
 * @param pcre_ops options for pcre2_compile
 * @param mapping An array of regex->serverlist mappings for filling in. Is cleared on error.
 * @param max_capcount_out The maximum detected pcre2 capture count is written here.
 */
void RegexHintFilter::form_regex_server_mapping(MXS_CONFIG_PARAMETER* params, int pcre_ops,
                                                MappingArray* mapping, uint32_t* max_capcount_out)
{
    ss_dassert(param_names_match_indexed.size() == param_names_target_indexed.size());
    bool error = false;
    uint32_t max_capcount = 0;
    *max_capcount_out = 0;
    /* The config parameters can be in any order and may be skipping numbers.
     * Must just search for every possibility. Quite inefficient, but this is
     * only done once. */
    for (unsigned int i = 0; i < param_names_match_indexed.size(); i++)
    {
        const char* param_name_match = param_names_match_indexed[i].c_str();
        const char* param_name_target = param_names_target_indexed[i].c_str();
        string match(config_get_string(params, param_name_match));
        string target(config_get_string(params, param_name_target));

        /* Check that both the regex and server config parameters are found */
        if (match.length() < 1 || target.length() < 1)
        {
            if (match.length() > 0)
            {
                MXS_ERROR("No server defined for regex setting '%s'.", param_name_match);
                error = true;
            }
            else if (target.length() > 0)
            {
                MXS_ERROR("No regex defined for server setting '%s'.", param_name_target);
                error = true;
            }
            continue;
        }

        if (!regex_compile_and_add(pcre_ops, false, match, target, mapping, &max_capcount))
        {
            error = true;
        }
    }

    if (error)
    {
        for (unsigned int i = 0; i < mapping->size(); i++)
        {
            pcre2_code_free(mapping->at(i).m_regex);
        }
        mapping->clear();
    }
    else
    {
        *max_capcount_out = max_capcount;
    }
}

/**
 * Check whether the client IP matches the configured 'source' host,
 * which can have up to three % wildcards.
 *
 * @param remote      The clientIP
 * @param ipv4        The client socket address struct
 * @return            1 for match, 0 otherwise
 */
int RegexHintFilter::check_source_host(const char* remote, const struct sockaddr_storage *ip)
{
    int ret = 0;
    struct sockaddr_in check_ipv4;

    memcpy(&check_ipv4, ip, sizeof(check_ipv4));

    switch (m_source->m_netmask)
    {
    case 32:
        ret = (m_source->m_address == remote) ? 1 : 0;
        break;
    case 24:
        /* Class C check */
        check_ipv4.sin_addr.s_addr &= 0x00FFFFFF;
        break;
    case 16:
        /* Class B check */
        check_ipv4.sin_addr.s_addr &= 0x0000FFFF;
        break;
    case 8:
        /* Class A check */
        check_ipv4.sin_addr.s_addr &= 0x000000FF;
        break;
    default:
        break;
    }

    ret = (m_source->m_netmask < 32) ?
          (check_ipv4.sin_addr.s_addr == m_source->m_ipv4.sin_addr.s_addr) :
          ret;

    if (ret)
    {
        MXS_INFO("Client IP %s matches host source %s%s",
                 remote,
                 m_source->m_netmask < 32 ? "with wildcards " : "",
                 m_source->m_address.c_str());
    }

    return ret;
}

/**
 * Validate IP address string againt three dots
 * and last char not being a dot.
 *
 * Match any, '%' or '%.%.%.%', is not allowed
 *
 */
bool RegexHintFilter::validate_ip_address(const char* host)
{
    int n_dots = 0;

    /**
     * Match any is not allowed
     * Start with dot not allowed
     * Host len can't be greater than INET_ADDRSTRLEN
     */
    if (*host == '%' ||
        *host == '.' ||
        strlen(host) > INET_ADDRSTRLEN)
    {
        return false;
    }

    /* Check each byte */
    while (*host != '\0')
    {
        if (!isdigit(*host) && *host != '.' && *host != '%')
        {
            return false;
        }

        /* Dot found */
        if (*host == '.')
        {
            n_dots++;
        }

        host++;
    }

    /* Check IPv4 max number of dots and last char */
    if (n_dots == 3 && (*(host - 1) != '.'))
    {
        return true;
    }
    else
    {
        return false;
    }
}

/**
 * Set the 'source' option into a proper struct. Input IP, which could have
 * wildcards %, is checked and the netmask 32/24/16/8 is added.
 *
 * @param input_host    The config source parameter
 * @return              The filled struct with netmask, or null on error
 */
SourceHost* RegexHintFilter::set_source_address(const char* input_host)
{
    ss_dassert(input_host);

    if (!input_host)
    {
        return NULL;
    }

    if (!validate_ip_address(input_host))
    {
        MXS_WARNING("The given 'source' parameter '%s' is not a valid IPv4 address.",
                    input_host);
        return NULL;
    }

    string address(input_host);
    struct sockaddr_in ipv4 = {};
    int netmask = 32;

    /* If no wildcards, leave netmask to 32 and return */
    if (!strchr(input_host, '%'))
    {
        return new SourceHost(address, ipv4, netmask);
    }

    char format_host[strlen(input_host) + 1];
    char* p = (char*)input_host;
    char* out = format_host;
    int bytes = 0;

    while (*p && bytes <= 3)
    {
        if (*p == '.')
        {
            bytes++;
        }

        if (*p == '%')
        {
            *out = bytes == 3 ? '1' : '0';
            netmask -= 8;

            out++;
            p++;
        }
        else
        {
            *out++ = *p++;
        }
    }

    *out = '\0';

    struct addrinfo *ai = NULL, hint = {};
    hint.ai_flags = AI_ADDRCONFIG | AI_V4MAPPED;
    int rc = getaddrinfo(input_host, NULL, &hint, &ai);

    /* fill IPv4 data struct */
    if (rc == 0)
    {
        ss_dassert(ai->ai_family == AF_INET);
        memcpy(&ipv4, ai->ai_addr, ai->ai_addrlen);

        /* if netmask < 32 there are % wildcards */
        if (netmask < 32)
        {
            /* let's zero the last IP byte: a.b.c.0 we may have set above to 1*/
            ipv4.sin_addr.s_addr &= 0x00FFFFFF;
        }

        MXS_INFO("Input %s is valid with netmask %d", address.c_str(), netmask);
        freeaddrinfo(ai);
    }
    else
    {
        MXS_WARNING("Found invalid IP address for parameter 'source=%s': %s",
                    input_host, gai_strerror(rc));
        return NULL;
    }

    return new SourceHost(address, ipv4, netmask);
}

/**
 * The module entry point routine. It is this routine that
 * must populate the structure that is referred to as the
 * "module object", this is a structure with the set of
 * external entry points for this module.
 *
 * @return The module object
 */
extern "C" MXS_MODULE* MXS_CREATE_MODULE()
{
    static MXS_FILTER_OBJECT MyObject = RegexHintFilter::s_object;

    static MXS_MODULE info =
    {
        MXS_MODULE_API_FILTER,
        MXS_MODULE_GA,
        MXS_FILTER_VERSION,
        "A routing hint filter that uses regular expressions to direct queries",
        "V1.1.0",
        RCAP_TYPE_CONTIGUOUS_INPUT,
        &MyObject,
        NULL, /* Process init. */
        NULL, /* Process finish. */
        NULL, /* Thread init. */
        NULL, /* Thread finish. */
        {
            {"source", MXS_MODULE_PARAM_STRING},
            {"user", MXS_MODULE_PARAM_STRING},
            {MATCH_STR, MXS_MODULE_PARAM_STRING},
            {SERVER_STR, MXS_MODULE_PARAM_SERVER},
            {
                "options",
                MXS_MODULE_PARAM_ENUM,
                "ignorecase",
                MXS_MODULE_OPT_NONE,
                option_values
            },
            {MXS_END_MODULE_PARAMS}
        }
    };

    /* This module takes parameters of the form match, match01, match02, ... matchN
     * and server, server01, server02, ... serverN. The total number of module
     * parameters is limited, so let's limit the number of matches and servers.
     * First, loop over the already defined parameters... */
    int params_counter = 0;
    while (info.parameters[params_counter].name != MXS_END_MODULE_PARAMS)
    {
        params_counter++;
    }

    /* Calculate how many pairs can be added. 100 is max (to keep the postfix
     * number within two decimals). */
    const int max_pairs = 100;
    int match_server_pairs = ((MXS_MODULE_PARAM_MAX - params_counter) / 2);
    if (match_server_pairs > max_pairs)
    {
        match_server_pairs = max_pairs;
    }
    ss_dassert(match_server_pairs >= 25); // If this limit is modified, update documentation.
    /* Create parameter pair names */
    generate_param_names(match_server_pairs);

    /* Now make the actual parameters for the module struct */
    MXS_MODULE_PARAM new_param_match = {NULL, MXS_MODULE_PARAM_STRING, NULL};
    /* Cannot use SERVERLIST in the target, since it may contain MASTER, SLAVE. */
    MXS_MODULE_PARAM new_param_target = {NULL, MXS_MODULE_PARAM_STRING, NULL};
    for (unsigned int i = 0; i < param_names_match_indexed.size(); i++)
    {
        new_param_match.name = param_names_match_indexed.at(i).c_str();
        info.parameters[params_counter] = new_param_match;
        params_counter++;
        new_param_target.name = param_names_target_indexed.at(i).c_str();
        info.parameters[params_counter] = new_param_target;
        params_counter++;
    }
    info.parameters[params_counter].name = MXS_END_MODULE_PARAMS;

    return &info;
}

/*
 * Generate N pairs of parameter names of form matchXX and targetXX and add them
 * to the global arrays.
 *
 * @param pairs The number of parameter pairs to generate
 */
static void generate_param_names(int pairs)
{
    const int namelen_match = sizeof(MATCH_STR) + 2;
    const int namelen_server = sizeof(TARGET_STR) + 2;

    char name_match[namelen_match];
    char name_server[namelen_server];

    const char FORMAT[] = "%s%02d";
    for (int counter = 1; counter <= pairs; counter++)
    {
        ss_debug(int rval = ) snprintf(name_match, namelen_match, FORMAT, MATCH_STR, counter);
        ss_dassert(rval == namelen_match - 1);
        ss_debug(rval = ) snprintf(name_server, namelen_server, FORMAT, TARGET_STR, counter);
        ss_dassert(rval == namelen_server - 1);

        // Have both names, add them to the global vectors
        param_names_match_indexed.push_back(name_match);
        param_names_target_indexed.push_back(name_server);
    }
}
