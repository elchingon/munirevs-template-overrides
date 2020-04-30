<?php

/**
 * @addtogroup SpideyDatabase
 * @{
 */

/**
 * @file
 * DEPRECATED functions, replaced with the DbConnection and related classes.
 */

/**
 * Connects to all database connections, except for those set to connect_on_demand
 */

function database_connect_all()
{
	global $database_connections;
	
	if(!isset($database_connections)) {
		ErrorHandler::fatal_error('Configuration file did not define $database_connections');
	}

	foreach($database_connections as $dbname => $db) {
		$database_connections[$dbname]['database_time'] = 0;

		if(!@$db['connect_on_demand'] && !defined('DATABASE_CONNECT_ALL_ON_DEMAND')) {
			database_connect($dbname);
		}
	}

}

/**
 * Connects to a database connection by name
 * @param string Database name
 * @param bool If true, and the database connection fails, will return instead of displaying the maintenance page.
 */
function database_connect($dbname, $allow_failure=false)
{
	global $database_connections;
	
	if(!isset($database_connections)) {
		ErrorHandler::fatal_error('Configuration file did not define $database_connections');
	}

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$dbname];
	
	// if an attempt has already been made to connect and it failed
	if(@$db['failed']) {
		return false;
	}

	switch($db['type']) {
	case 'memcache':
		$c = false;

		$begin_time = microtime(true);
		foreach($db['servers'] as $key => $server) {
			if(!$c) {
				if(!@$db['non-persistent']) {
					$c = memcache_pconnect($server['hostname'], $server['port']);
				} else {
					$c = memcache_connect($server['hostname'], $server['port']);
				}
			} else {
				memcache_add_server($c, $server['hostname'], $server['port'], (@$db['non-persistent'] ? false : true), $server['weight']);
			}
		}
		$end_time = microtime(true);

		$database_connections[$dbname]['database_time'] += ($end_time-$begin_time);

		if(is_bool($c) && count($db['servers'])) {
			ErrorHandler::message($server, 'database_connect_all(): Memcache connection failed', E_ERROR);
		} else if(!is_bool($c)) {
			$database_connections[$dbname]['memcache_connection'] = $c;
		}
		$database_connections[$dbname]['connected'] = true;
		break;

	case 'mysql':
		$begin_time = microtime(true);
		
		$connect_string = $db['hostname'] . (isset($db['port']) ? ':' . $db['port'] : (isset($db['socket']) ? ':' . $db['socket'] : ''));

		if(!@$db['non-persistent']) {
			$c = @mysql_pconnect($connect_string, $db['username'], $db['password'], true);
		} else {
			$c = @mysql_connect($connect_string, $db['username'], $db['password'], true);
		}

		if($c === false) {
			$msg = "mysql_connect(): " . mysql_error();
			if(@$db['non-fatal-failure'] || $allow_failure) {
				ErrorHandler::message(get_defined_vars(), $msg, E_ERROR);
				$database_connections[$dbname]['failed'] = true;
				return false;
			} else {
				ErrorHandler::fatal_error('Could not connect to ' . $dbname);
			}
			break;
		} else {
			if(!@mysql_select_db($db['database'], $c)) {
				$msg = "mysql_select_db(): " . mysql_error();
				if(@$db['non-fatal-failure'] || $allow_failure) {
					ErrorHandler::message($msg, E_ERROR);
					$database_connections[$dbname]['failed'] = true;
					return false;
				} else {
					ErrorHandler::fatal_error('Could not connect to ' . $dbname);
				}
			}
		}
		$end_time = microtime(true);

		$database_connections[$dbname]['database_time'] += ($end_time-$begin_time);

		$database_connections[$dbname]['mysql_connection'] = $c;
		$database_connections[$dbname]['connected'] = true;
		break;

	case 'mysqli':
		$begin_time = microtime(true);
		
		$mysqli = mysqli_init();
		$r = $mysqli->real_connect($db['hostname'], $db['username'], $db['password'], $db['database'], 
				(isset($db['port']) ? $db['port'] : false), 
				(isset($db['socket']) ? $db['socket'] : false),
				0);
		
		if($r === false) {
			$msg = "mysqli_connect(): " . $mysqli->connect_error;
			if(@$db['non-fatal-failure'] || $allow_failure) {
				ErrorHandler::message(get_defined_vars(), $msg, E_ERROR);
				$database_connections[$dbname]['failed'] = true;
				return false;
			} else {
				ErrorHandler::fatal_error('Could not connect to ' . $dbname);
			}
		}
		$end_time = microtime(true);
		
		$mysqli->set_charset('utf8');

		$database_connections[$dbname]['database_time'] += ($end_time-$begin_time);
	
		$database_connections[$dbname]['mysqli_connection'] = $mysqli;
		$database_connections[$dbname]['connected'] = true;
		break;

	default:
		$msg = "database_connect(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.";
		if(@$db['non-fatal-failure'] || $allow_failure) {
			ErrorHandler::message(get_defined_vars(), $msg, E_ERROR);
		} else {
			ErrorHandler::fatal_error('Could not connect to ' . $dbname);
		}
		return false;
	}

	return true;
}

function database_close($dbname)
{
	global $database_connections;
	
	if(!isset($database_connections)) {
		ErrorHandler::fatal_error('Configuration file did not define $database_connections');
	}

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}
	
	$db = $database_connections[$dbname];
	
	if(!@$db['connected']) {
		return;
	}
	
	switch($db['type']) {
	case 'memcache':
		if(isset($db['non-persistent']) && isset($db['connected'])) {
			memcache_close($db['memcache_connection']);
			$database_connections[$dbname]['connected'] = false;
		}
		break;
	
	case 'mysql':
		if(@$db['non-persistent'] && isset($db['connected'])) {
			mysql_close($db['mysql_connection']);
			$database_connections[$dbname]['connected'] = false;
		}
		break;

	case 'mysqli':
		if(isset($db['connected'])) {
			$db['mysqli_connection']->close();
			$database_connections[$dbname]['connected'] = false;
		}
		break;

	default:
		ErrorHandler::fatal_error("database_close(): database type '" . $db['type'] . "' for connection '" . $name . "' not supported.");
	}

}

function database_close_all()
{
	global $database_connections;

	foreach($database_connections as $name => $db) {
		database_close($name);
	}
}

function database_flush_cache($dbname)
{
	global $database_connections;

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$dbname];

	switch($db['type']) {
	case 'memcache':
		memcache_flush($db['memcache_connection']);
		break;

	default:
		ErrorHandler::message(get_defined_vars(), "database_flush_cache(): database type '" . $db['type'] . "' for connection '" . $dbname . "' does not support flushing.", E_ERROR, -1);
		break;
	}
}

function database_query($query, $description, $dbname, $ignore_warnings=false, $no_debug_message=false, $recursion_level=0)
{
	global $database_connections;

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$dbname];
	
	if(!@$db['connected']) {
		database_connect($dbname);
		$db = $database_connections[$dbname];
	}
	
	if(!@$db['connected']) {
		return false;
	}
	
	switch($db['type']) {
	case 'memcache':
		if(isset($db['memcache_connection'])) {
			$begin_time = microtime(true);

			// calculate the key for this query (including the hostname and database name of the real database) 
			$cache_key = hash($db['hash_algorithm'], $database_connections[$db['database_connection']]['hostname'] . 
				'-' . $database_connections[$db['database_connection']]['database'] . 
				'-' . $query);
			ErrorHandler::message($cache_key, "memcache key calculated");
		
			// check the first server that is connected for the data
			$rows = memcache_get($db['memcache_connection'], $cache_key);
		
			$end_time = microtime(true);

			$database_connections[$dbname]['database_time'] += ($end_time-$begin_time);
		}

		// if not found in memcache
		if(!is_array(@$rows)) {
			// run the query on the real database connection.
			$result = database_query($query, 'FOR MEMCACHE: ' . $description, $db['database_connection'], $ignore_warnings);
			if(database_is_error($result)) {
				return $result;
			}

			// fetch all of the rows
			$rows = array();
			while($row = database_fetch_row($result)) {
				$rows[] = $row;
			}

			// cache the resulting rows
			if(isset($db['memcache_connection'])) {
				$begin_time = microtime(true);
				memcache_set($db['memcache_connection'], $cache_key, $rows, 0, (60*mt_rand($db['min_cache_time'],$db['max_cache_time'])));  // random number of minutes
				$end_time = microtime(true);

				$database_connections[$dbname]['database_time'] += ($end_time-$begin_time);
			}
		} else {
			$str = count($rows) . " rows";

			ErrorHandler::message(array('query' => $query), $dbname . " Query (" . $str . ", " . number_format($end_time-$begin_time, 3) . " sec): " . $description, E_DEBUG, -1);
		}

		return array('dbname' => $dbname, 'rows' => $rows, 'row_index' => 0);

	case 'mysql':
		$connection = $db['mysql_connection'];

		$begin_time = microtime(true);
		$result = mysql_query($query, $connection);
		$end_time = microtime(true);

		$database_connections[$dbname]['database_time'] += ($end_time-$begin_time);

		if(!is_bool($result)) {
			$num_rows = mysql_num_rows($result);
			$str = $num_rows . " rows";
		} else {
			$affected = mysql_affected_rows($connection);
			$str = $affected . " affected";
		}

		if($result === false) {
			// if the error is "server gone away" or "lost connection to server during query"
			if($mysqli->errno == 2006 || $mysqli->errno == 2013) {
				
				if($recursion_level > 5) {
					ErrorHandler::message(get_defined_vars(), $dbname . " connection failed after reconnecting too many times, I'm done trying.", E_ERROR);
					return false;
				}
				
				ErrorHandler::message(get_defined_vars(), $dbname . " connection to server failed. Going to attempt reconnect and run query again.", E_WARNING);
				
				while(!database_connect($dbname)) {
					sleep(mt_rand(2, 12));
				}
				
				return database_query($query, $description, $dbname, $ignore_warnings, $no_debug_message, $recursion_level+1);
				
			} else {
				ErrorHandler::message(array('error' => mysql_error($connection), 'query' => $query), "MySQL ERROR: " . $description, E_WARNING, -1);
			}
		} else {
			if(!$no_debug_message) {
				ErrorHandler::message(array('query' => $query), $dbname . " Query (" . $str . ", " . number_format($end_time-$begin_time, 3) . " sec): " . $description, E_DEBUG, -1);
			}
		}
		return array('dbname' => $dbname, 'result' => $result, 'num_rows' => @$num_rows, 'affected_rows' => @$affected);

	case 'mysqli':
		$mysqli = $db['mysqli_connection'];

		$begin_time = microtime(true);
		$result = $mysqli->query($query);
		$end_time = microtime(true);
		
		$database_connections[$dbname]['database_time'] += ($end_time-$begin_time);
		
		if(!is_bool($result)) {
			$num_rows = mysqli_num_rows($result);
			$str = $num_rows . " rows";
		} else {
			$affected = $mysqli->affected_rows;
			$str = $affected . " affected";
		}
		
		// if the query failed
		if($result === false) {
			// if the error is "server gone away" or "lost connection to server during query"
			if($mysqli->errno == 2006 || $mysqli->errno == 2013) {
				
				if($recursion_level > 5) {
					ErrorHandler::message(get_defined_vars(), $dbname . " connection failed after reconnecting too many times, I'm done trying.", E_ERROR);
					return false;
				}
				
				ErrorHandler::message(get_defined_vars(), $dbname . " connection to server failed. Going to attempt reconnect and run query again.", E_WARNING);
				
				while(!database_connect($dbname)) {
					sleep(mt_rand(2, 12));
				}
				
				return database_query($query, $description, $dbname, $ignore_warnings, $no_debug_message, $recursion_level+1);
				
			// look for an error reported directly by a stored procedure, etc.
			} else if($ret = preg_match('/PROCEDURE .+\.Error([a-zA-Z]+) does not exist/', $mysqli->error, $matches)) {
				// report it as a warning
				ErrorHandler::message(array('error' => $mysqli->error, 'query' => $query), "MySQLi stored procedure said " . $matches[1], E_WARNING, -1);
				
			} else {
				ErrorHandler::message(array('error' => $mysqli->error, 'query' => $query), "MySQLi ERROR: " . $description, E_WARNING, -1);
			}
		} else {
			if($mysqli->warning_count && !$ignore_warnings) {
				$warnings = database_get_warnings($dbname);
				ErrorHandler::message(array('query' => $query, 'warnings' => $warnings), $dbname . " Query (" . $str . ", " . number_format($end_time-$begin_time, 3) . " sec, " . count($warnings) . " warnings): " . $description, E_WARNING, -1);
			} else {
				if(!$no_debug_message) {
					ErrorHandler::message(array('query' => $query), $dbname . " Query (" . $str . ", " . number_format($end_time-$begin_time, 3) . " sec): " . $description, E_DEBUG, -1);
				}
			}
		}
		return array('dbname' => $dbname, 'result' => $result, 'num_rows' => @$num_rows, 'affected_rows' => @$affected);

	default:
		ErrorHandler::message(get_defined_vars(), "database_query(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.", E_ERROR, -1);
		break;
	}
}

/**
 * Allows multiple queries in one string, separated by semicolons.
 * Currently this does not support result sets, so only use it for queries that do not return data.
 */
function database_multi_query($query, $description, $dbname, $ignore_warnings=false, $no_debug_message=false)
{
	global $database_connections;

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$dbname];
	
	if(!@$db['connected']) {
		database_connect($dbname);
		$db = $database_connections[$dbname];
	}
	
	if(!@$db['connected']) {
		return false;
	}
	
	switch($db['type']) {
	case 'mysqli':
		$mysqli = $db['mysqli_connection'];

		$begin_time = microtime(true);
		$resultcode = $mysqli->multi_query($query);
		$end_time = microtime(true);

		$database_connections[$dbname]['database_time'] += ($end_time-$begin_time);
		
		$results = array();
		$count = 1;
		do {
			if($resultcode === false) {
				ErrorHandler::message(array('error' => $mysqli->error, 'query' => $query), 
						"MySQLi ERROR: " . $description . " (part " . $count . ")", 
						E_ERROR, -1);
				$mysqliresult = false;
			} else {
				$mysqliresult = $mysqli->store_result();
				
				if($mysqliresult === false) {
					$mysqliresult = true;
					$affected = $mysqli->affected_rows;
					$str = $affected . " affected";
				} else {
					$num_rows = $mysqliresult->num_rows;
					$str = $num_rows . " rows";
				}
				
				if(!$no_debug_message) {
					ErrorHandler::message(array('query' => $query), 
							$dbname . " Query (" . $str . ", " . number_format($end_time-$begin_time, 3) . " sec): " . $description . " (part " . $count . ")", 
							E_DEBUG, -1);
				}
				
			}
			
			$count++;
			
			$results[] = array('dbname' => $dbname, 'result' => $mysqliresult, 'num_rows' => @$num_rows, 'affected_rows' => @$affected);
			
			if(!$mysqli->more_results()) {
				break;
			}
			
			$resultcode = $mysqli->next_result();
		} while(1);

		return $results;

	default:
		ErrorHandler::message(get_defined_vars(), "database_multi_query(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.", E_ERROR, -1);
		break;
	}
}

function database_free_result($result)
{
	global $database_connections;

	if(!is_array($result)) {
		return true;
	}

	if(!isset($database_connections[$result['dbname']])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$result['dbname']];

	switch($db['type']) {
	case 'memcache':
		break;
	
	case 'mysql':
		mysql_free_result($result['result']);
		break;
	
	case 'mysqli':
		mysqli_free_result($result['result']);
		break;
		
	default:
		ErrorHandler::message(get_defined_vars(), "database_free_result(): database type '" . $db['type'] . "' for connection '" . $result['dbname'] . "' not supported.", E_ERROR, -1);
		break;
	}
}

/**
 * Performs the MySQL query given and returns an array of the rows returned (each is an associative array).
 * If by_id_col is supplied, the column name provided will be used as the key in the returned array.
 */
function database_query_rows($query, $description, $dbname, $by_id_col=false, $ignore_warnings=false, $no_debug_message=false)
{
	global $database_connections;

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$dbname];
	
	$result = database_query($query, $description, $dbname, $ignore_warnings, $no_debug_message);

	if(database_is_error($result)) {
		return false;
	}

	switch($db['type']) {
	case 'memcache':
		if($by_id_col === false) {
			return $result['rows'];
		} else {
			$rows = array();
			foreach($result['rows'] as $row) {
				$rows[$row[$by_id_col]] = $row;
			}
			database_free_result($result);
			return $rows;
		}

	case 'mysql':
	case 'mysqli':
		$rows = array();
		while($row = database_fetch_row($result)) {
			if($by_id_col) {
				$rows[$row[$by_id_col]] = $row;
			} else {
				$rows[] = $row;
			}
		}
		database_free_result($result);
		return $rows;
	}
}

function database_is_error($result)
{
	global $database_connections;
	
	if(!is_array($result)) {
		return true;
	}

	if(!isset($database_connections[$result['dbname']])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return true;
	}

	$db = $database_connections[$result['dbname']];
	
	
	switch($db['type']) {
	case 'memcache':
		if(!isset($result['rows'])) {
			return true;
		} else {
			return false;
		}
	
	case 'mysql':
	case 'mysqli':
		return $result['result'] === false;
	}
}

function database_get_error($result)
{
	global $database_connections;

	if(!isset($database_connections[$result['dbname']])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$result['dbname']];
	
	
	switch($db['type']) {
	case 'mysql':
		return mysql_error($db['mysql_connection']);

	case 'mysqli':
		return $db['mysqli_connection']->error;

	default:
		ErrorHandler::message(get_defined_vars(), "database_get_error(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.", E_ERROR, -1);
		return false;
	}
}

function database_is_duplicate_insert($result)
{
	global $database_connections;

	if(!isset($database_connections[$result['dbname']])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$result['dbname']];
	
	switch($db['type']) {
	case 'mysql':
	case 'mysqli':
		return database_get_errno($result) == 1062;
		
	default:
		ErrorHandler::message(get_defined_vars(), "database_is_duplicate_insert(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.", E_ERROR, -1);
		return false;
	}
}

function database_get_errno($result)
{
	global $database_connections;

	if(!isset($database_connections[$result['dbname']])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$result['dbname']];
	
	
	switch($db['type']) {
	case 'mysql':
		return mysql_errno($db['mysql_connection']);

	case 'mysqli':
		return $db['mysqli_connection']->errno;

	default:
		ErrorHandler::message(get_defined_vars(), "database_get_errno(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.", E_ERROR, -1);
		return false;
	}
}

function database_get_warnings($dbname)
{
	global $database_connections;

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$dbname];
	
	switch($db['type']) {
	case 'mysql':
		ErrorHandler::message(get_defined_vars(), "database_get_warnings: database type '" . $db['type'] . "' doesn't support warnings", E_WARNING, -1);
		return false;

	case 'mysqli':
		return database_query_rows("SHOW WARNINGS", 'get warnings', $dbname, false, true, true);

	default:
		ErrorHandler::message(get_defined_vars(), "database_get_warnings(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.", E_ERROR, -1);
		return false;
	}
}

function database_num_rows($result)
{
	global $database_connections;

	if(!isset($database_connections[$result['dbname']])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$result['dbname']];
	
	switch($db['type']) {
	case 'memcache':
		return count($result['rows']);
	
	case 'mysql':
		return $result['num_rows'];
	
	case 'mysqli':
		return $result['num_rows'];
		
	default:
		ErrorHandler::message(get_defined_vars(), "database_num_rows(): database type '" . $db['type'] . "' for connection '" . $result['dbname'] . "' not supported.", E_ERROR, -1);
		break;
	}
}

function database_data_seek(&$result, $offset)
{
	global $database_connections;

	if(!isset($database_connections[$result['dbname']])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$result['dbname']];
	
	switch($db['type']) {
	case 'memcache':
		if($offset < 0 || $offset >= count($result['rows'])) {
			ErrorHandler::message($result, 'database_data_seek(): new index not valid (memcache)', E_ERROR);
			return false;
		}
	
		$result['row_index'] = $offset;
		return true;

	case 'mysql':
		$ret = mysql_data_seek($result['result'], $offset);
		if(!$ret) {
			ErrorHandler::message($result, 'database_data_seek(): mysql_data_seek() failed', E_ERROR);
		}
		return $ret;

	case 'mysqli':
		$ret = mysqli_data_seek($result['result'], $offset);
		if(!$ret) {
			ErrorHandler::message($result, 'database_data_seek(): mysqli_data_seek() failed', E_ERROR);
		}
		return $ret;

	default:
		ErrorHandler::message(get_defined_vars(), "database_data_seek(): database type '" . $db['type'] . "' for connection '" . $result['dbname'] . "' not supported.", E_ERROR, -1);
		break;
	}
}

function database_affected_rows($result)
{
	global $database_connections;

	if(!isset($database_connections[$result['dbname']])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$result['dbname']];
	
	switch($db['type']) {
	case 'memcache':
		ErrorHandler::message(get_defined_vars(), "database_affected_rows(): database type '" . $db['type'] . "' does not support affecting rows.", E_ERROR, -1);
		return false;

	case 'mysql':
		return $result['affected_rows'];

	case 'mysqli':
		return $result['affected_rows'];

	default:
		ErrorHandler::message(get_defined_vars(), "database_affected_rows(): database type '" . $db['type'] . "' for connection '" . $result['dbname'] . "' not supported.", E_ERROR, -1);
		break;
	}
}

function database_insert_id($result)
{
	global $database_connections;

	if(!isset($database_connections[$result['dbname']])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$result['dbname']];

	switch($db['type']) {
	case 'memcache':
		ErrorHandler::message(get_defined_vars(), "database_insert_id(): database type '" . $db['type'] . "' does not support inserting rows.", E_ERROR, -1);
		return false;

	case 'mysql':
		return mysql_insert_id($db['mysql_connection']);

	case 'mysqli':
		//return $db['mysqli_connection']->insert_id;

$q = "SELECT LAST_INSERT_ID() as lastid";
$res = database_query($q,"last_id",$result['dbname']);
$row = database_fetch_row($res);
$last_id = $row['lastid'];
return $last_id;


	default:
		ErrorHandler::message(get_defined_vars(), "database_insert_id(): database type '" . $db['type'] . "' for connection '" . $result['dbname'] . "' not supported.", E_ERROR, -1);
		break;
	}
}

function database_fetch_row(&$result)
{
	global $database_connections;

	if(!isset($database_connections[$result['dbname']])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$result['dbname']];
	
	switch($db['type']) {
	case 'memcache':
		if($result['row_index'] >= count($result['rows'])) {
			return false;
		}

		$row = $result['rows'][$result['row_index']];
		$result['row_index']++;

		return $row;

	case 'mysql':
		$begin_time = microtime(true);
		$row = mysql_fetch_assoc($result['result']);
		$end_time = microtime(true);

		$database_connections[$result['dbname']]['database_time'] += ($end_time-$begin_time);
		return $row;
		
	case 'mysqli':
		$begin_time = microtime(true);
		$row = mysqli_fetch_assoc($result['result']);
		$end_time = microtime(true);

		$database_connections[$result['dbname']]['database_time'] += ($end_time-$begin_time);
		return $row;
		
	default:
		ErrorHandler::message(get_defined_vars(), "database_fetch_row(): database type '" . $db['type'] . "' for connection '" . $result['dbname'] . "' not supported.", E_ERROR, -1);
		break;
	}
}


function database_is_in_transaction($dbname)
{
	global $database_connections;

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$dbname];
	
	switch($db['type']) {
	case 'mysql':
	case 'mysqli':
		return @$db['in_transaction'] ? true : false;
		
	default:
		ErrorHandler::message(get_defined_vars(), "database_is_in_transaction(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.", E_WARNING, -1);
		return false;
	}
}


function database_transaction_start($dbname)
{
	global $database_connections;

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$dbname];
	
	switch($db['type']) {
	case 'mysql':
	case 'mysqli':
		if(@$db['in_transaction']) {
			ErrorHandler::message($dbname, "Already in the middle of transaction, can't start new one", E_ERROR);
			return false;
		} else {
			$result = database_query("START TRANSACTION", 'start transaction', $dbname);
			if(database_is_error($result)) {
				return false;
			} else {
				$database_connections[$dbname]['in_transaction'] = true;
				return true;
			}
		}
		break;
		
	default:
		ErrorHandler::message(get_defined_vars(), "database_transaction_start(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.", E_ERROR, -1);
		break;
	}
}


function database_transaction_commit($dbname)
{
	global $database_connections;

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$dbname];
	
	switch($db['type']) {
	case 'mysql':
	case 'mysqli':
		if(!@$db['in_transaction']) {
			ErrorHandler::message($dbname, "Not in a transaction, can't commit", E_ERROR);
			return false;
		} else {
			$result = database_query("COMMIT", 'commit', $dbname);
			if(database_is_error($result)) {
				return false;
			} else {
				$database_connections[$dbname]['in_transaction'] = false;
				return true;
			}
		}
		break;
		
	default:
		ErrorHandler::message(get_defined_vars(), "database_transaction_commit(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.", E_ERROR, -1);
		break;
	}
}


function database_transaction_rollback($dbname)
{
	global $database_connections;

	if(!isset($database_connections[$dbname])) {
		ErrorHandler::message($dbname, "Database connection doesn't exist", E_ERROR, 1);
		return false;
	}

	$db = $database_connections[$dbname];
	
	switch($db['type']) {
	case 'mysql':
	case 'mysqli':
		if(!@$db['in_transaction']) {
			ErrorHandler::message($dbname, "Not in a transaction, can't rollback", E_ERROR);
			return false;
		} else {
			$result = database_query("ROLLBACK", 'rollback', $dbname);
			if(database_is_error($result)) {
				return false;
			} else {
				$database_connections[$dbname]['in_transaction'] = false;
				return true;
			}
		}
		break;
		
	default:
		ErrorHandler::message(get_defined_vars(), "database_transaction_commit(): database type '" . $db['type'] . "' for connection '" . $dbname . "' not supported.", E_ERROR, -1);
		break;
	}
}


/** @} */
