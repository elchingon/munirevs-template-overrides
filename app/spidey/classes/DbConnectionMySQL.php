<?php

/**
 * @ingroup SpideyDatabase
 * Connects to a MySQL database and performs all interaction with the database.
 */

class DbConnectionMySQL extends DbConnection {
	
	protected $database_time = 0;
	protected $mysqli_connection = null;
	protected $in_transaction = false;
	protected $failed = false;
	
	function __construct($dbname, $allow_failure=false)
	{
		parent::__construct($dbname);
		
		// if the dbinfo contains another object for the same connection, get its mysql connection.
		if(isset($this->dbinfo['object'])) {
			$this->mysqli_connection = $this->dbinfo['object']->mysqli_connection;
		}
		
		$this->connect($allow_failure);
		
	}
	
	public function prepval($value)
	{
		if($value === null) {
			return 'NULL';
		} else if($value === self::NOW) {
			return 'NOW()';
		} else {
			return "'" . $this->mysqli_connection->escape_string($value) . "'";
		}
	}
	
	/**
	 * Returns a string that can be used after a field name to compare.
	 * @param string Comparison operator. Can be anything MySQL accepts. If "=" and value is null, the return is "IS NULL".
	 *               If "!=" and value is null, return is "IS NOT NULL".
	 * @param string Value to compare with. Will be escaped and wrapped in quotes, unless null.
	 * @return string.
	 */
	public function prep_compare_val($compare, $value)
	{
		if($value === null) {
			if($compare == '=') {
				return 'IS NULL';
			}
			if($compare == '!=') {
				return 'IS NOT NULL';
			}
		} else {
			return $compare . "'" . addslashes($value) . "'";
		}
	}
	
	/**
	 * Takes binary data, returning a string that can be used in MySQL queries.
	 * @param binary string
	 * @return string Escaped and quoted string in HEX format.
	 */
	public function prepval_hex($value)
	{
		if($value === null) {
			return 'NULL';
		} else {
			$chars = str_split($value, 1);
			array_walk($chars, function(&$char) {
				$char = sprintf('%02X', ord($char));
			});
			return "X'" . implode($chars) . "'";
		}
	}
	
	/**
	 * Connects to a database connection by name
	 * @param bool If true, and the database connection fails, will return instead of displaying the maintenance page.
	 * @throws DbException
	 */
	function connect($allow_failure=false)
	{
		
		$begin_time = microtime(true);
		
		$this->mysqli_connection = mysqli_init();
		
		$r = $this->mysqli_connection->real_connect($this->dbinfo['hostname'], $this->dbinfo['username'], $this->dbinfo['password'], $this->dbinfo['database'], 
				(isset($this->dbinfo['port']) ? $this->dbinfo['port'] : false), 
				(isset($this->dbinfo['socket']) ? $this->dbinfo['socket'] : false),
				0);
		
		if($r === false) {
			$msg = "DbConnectionMySQL::connect(): " . $this->mysqli_connection->connect_error;
			if(@$this->dbinfo['non-fatal-failure'] || $allow_failure) {
				ErrorHandler::message(get_defined_vars(), $msg, E_ERROR);
				throw new DbException($this->mysqli_connection->connect_error);
			} else {
				ErrorHandler::fatal_error('Could not connect to ' . $this->dbname);
			}
		}
		
		$this->mysqli_connection->set_charset('utf8');
		
		$end_time = microtime(true);
		$this->add_database_time($end_time-$begin_time);
		
		$this->connected = true;
		
		if(isset($this->dbinfo['set_timezone'])) {
			$result = $this->query("SET time_zone = " . $this->prepval($this->dbinfo['set_timezone']), 'set timezone');
		}
		
		return true;
	}
	
	public function disconnect()
	{
		$this->mysqli_connection->close();
		$this->connected = false;
	}
	
	public function get_insert_id()
	{
		return $this->mysqli_connection->insert_id;
	}
	
	public function get_error()
	{
		return $this->mysqli_connection->error;
	}
	
	public function get_errno()
	{
		return $this->mysqli_connection->errno;
	}
	
	/**
	 * Returns the name of the database or schema in use by this connection.
	 * @return string
	 */
	public function get_database_name()
	{
		return $this->dbinfo['database'];
	}
	
	public function add_database_time($time)
	{
		$this->database_time += $time;
	}
	
	/**
	 * Runs a single query.
	 * @param string The query.
	 * @param string Description of the query.
	 * @param bool Whether to ignore any warnings that occur.
	 * @param bool If true, no debug message will be output.
	 * @throws DbQueryException
	 * @throws DbException
	 * @return DbResult object
	 */
	public function query($query, $description, $ignore_warnings=false, $no_debug_message=false, $recursion_level = 0)
	{
		
		if(!$this->connected) {
			$this->connect();
		}
		
		$begin_time = microtime(true);
		$mysqli_result = $this->mysqli_connection->query($query);
		$end_time = microtime(true);
		
		$elapsed_time = ($end_time-$begin_time);
		$this->add_database_time($elapsed_time);
		
		if(!is_bool($mysqli_result)) {
			$num_rows = $mysqli_result->num_rows;
			$affected_rows = 0;
			$str = $num_rows . " rows";
		} else {
			$num_rows = 0;
			$affected_rows = $this->mysqli_connection->affected_rows;
			$str = $affected_rows . " affected";
		}

		$last_insert_id = $this->get_insert_id();

		if($mysqli_result === false) {
			// if the error is "server gone away" or "lost connection to server during query"
			if($this->mysqli_connection->errno == 2006 || $this->mysqli_connection->errno == 2013) {
				
				if($recursion_level > 5) {
					ErrorHandler::message(get_defined_vars(), $this->dbname . " connection failed after reconnecting too many times, I'm done trying.", E_ERROR);
					throw new DbConnectionLostException($this->dbname);
				}
				
				ErrorHandler::message(get_defined_vars(), $this->dbname . " connection to server failed. Going to attempt reconnect and run query again.", E_WARNING);
				
				while(!$this->connect()) {
					sleep(mt_rand(2, 12));
				}
				
				return $this->query($query, $description, $ignore_warnings, $no_debug_message, $recursion_level+1);
				
			} else {
				ErrorHandler::message(array('error' => $this->mysqli_connection->error, 'query' => $query), "MySQLi ERROR: " . $description, E_WARNING, -1);
				throw new DbQueryException($this->mysqli_connection->error);
			}
		} else {
			if($this->mysqli_connection->warning_count && !$ignore_warnings) {
				$warnings = $this->get_warnings();
				ErrorHandler::message(array('query' => $query, 'warnings' => $warnings), $this->dbname . " Query (" . $str . ", " . number_format($end_time-$begin_time, 3) . " sec, " . count($warnings) . " warnings): " . $description, E_WARNING, -1);
			} else {
				if(!$no_debug_message) {
					ErrorHandler::message(array('query' => $query), $this->dbname . " Query (" . $str . ", " . number_format($end_time-$begin_time, 3) . " sec): " . $description, E_DEBUG, -1);
				}
			}
		}
		
		$result = new DbResultMySQL($this, $mysqli_result, $num_rows, $affected_rows);
		$result->set_last_insert_id($last_insert_id);
		
		$result->set_elapsed_time($elapsed_time);
		
		return $result;
	}
	
	/**
	 * Runs a single query that is not retrieved and stored locally. This runs mysqli_real_query and mysqli_use_result.
	 * @param string The query.
	 * @param string Description of the query.
	 * @param bool Whether to ignore any warnings that occur.
	 * @param bool If true, no debug message will be output.
	 * @throws DbQueryException
	 * @throws DbException
	 * @return DbResult object
	 */
	public function query_nonstored($query, $description, $ignore_warnings=false, $no_debug_message=false, $recursion_level = 0)
	{
		
		if(!$this->connected) {
			$this->connect();
		}
		
		$begin_time = microtime(true);
		$resultcode = $this->mysqli_connection->real_query($query);
		$end_time = microtime(true);
		
		$elapsed_time = ($end_time-$begin_time);
		$this->add_database_time($elapsed_time);
		
		$results = array();
		$count = 1;
		
		if($resultcode === false) {
			ErrorHandler::message(array('error' => $this->mysqli_connection->error, 'query' => $query), 
					"MySQLi ERROR: " . $description . " (part " . $count . ")", 
					E_WARNING, -1);
			
			throw new DbQueryException($this->mysqli_connection->error);
		}
		
		$mysqli_result = $this->mysqli_connection->use_result();
		
		if($mysqli_result === false) {
			$mysqli_result = true;
			$num_rows = 0;
			$affected_rows = $this->mysqli_connection->affected_rows;
			$str = $affected_rows . " affected";
		} else {
			$num_rows = $mysqli_result->num_rows;
			$affected_rows = 0;
			$str = $num_rows . " rows";
		}
		
		if(!$no_debug_message) {
			ErrorHandler::message(array('query' => $query), 
					$this->dbname . " Query (" . $str . ", " . number_format($end_time-$begin_time, 3) . " sec): " . $description . " (part " . $count . ")", 
					E_DEBUG, -1);
		}
		
		$result = new DbResultMySQL($this, $mysqli_result, $num_rows, $affected_rows);
		
		$result->set_elapsed_time($elapsed_time);
		
		return $result;
	}
	
	/**
	 * Allows multiple queries in one string, separated by semicolons.
	 * @param string The query.
	 * @param string Description of the query.
	 * @param bool Whether to ignore any warnings that occur.
	 * @param bool If true, no debug message will be output.
	 * @throws DbException
	 * @return array of DbResult object or DbQueryException object for each query in the string
	 */
	public function multi_query($query, $description, $ignore_warnings=false, $no_debug_message=false)
	{
		
		if(!$this->connected) {
			$this->connect();
		}
		
		$begin_time = microtime(true);
		$resultcode = $this->mysqli_connection->multi_query($query);
		$end_time = microtime(true);
		
		$elapsed_time = ($end_time-$begin_time);
		$this->add_database_time($elapsed_time);
		
		$results = array();
		$count = 1;
		do {
			if($resultcode === false) {
				ErrorHandler::message(array('error' => $this->mysqli_connection->error, 'query' => $query), 
						"MySQLi ERROR: " . $description . " (part " . $count . ")", 
						E_WARNING, -1);
				
				$results[] = new DbQueryException($this->mysqli_connection->error);
			} else {
				$mysqli_result = $this->mysqli_connection->store_result();
				
				if($mysqli_result === false) {
					$mysqli_result = true;
					$num_rows = 0;
					$affected_rows = $this->mysqli_connection->affected_rows;
					$str = $affected_rows . " affected";
				} else {
					$num_rows = $mysqli_result->num_rows;
					$affected_rows = 0;
					$str = $num_rows . " rows";
				}
			
				if(!$no_debug_message) {
					ErrorHandler::message(array('query' => $query), 
							$this->dbname . " Query (" . $str . ", " . number_format($end_time-$begin_time, 3) . " sec): " . $description . " (part " . $count . ")", 
							E_DEBUG, -1);
				}
				
				$results[] = $result = new DbResultMySQL($this, $mysqli_result, $num_rows, $affected_rows);
				$result->set_elapsed_time($elapsed_time);
				
			}
			
			$count++;
			
			if(!$this->mysqli_connection->more_results()) {
				break;
			}
			
			$resultcode = $this->mysqli_connection->next_result();
		} while(1);
		
		return $results;
	}
	
	/**
	 * Performs the MySQL query given and returns an array of the rows returned (each is an associative array).
	 * @param string The query.
	 * @param string Description of the query.
	 * @param string (optional) If supplied, the column name provided will be used as the key in the returned array. Null otherwise.
	 * @param bool Whether to ignore any warnings that occur.
	 * @param bool If true, no debug message will be output.
	 */
	public function query_rows($query, $description, $by_id_col=null, $ignore_warnings=false, $no_debug_message=false)
	{
		
		$result = $this->query($query, $description, $ignore_warnings, $no_debug_message);
		
		if($result->is_error()) {
			return false;
		}
		
		$rows = array();
		while($row = $result->fetch_row()) {
			if($by_id_col !== null) {
				$rows[$row[$by_id_col]] = $row;
			} else {
				$rows[] = $row;
			}
		}
		
		return $rows;
	}

	public function get_warnings()
	{
		return $this->query_rows("SHOW WARNINGS", 'get warnings', null, true, true);
	}

	public function is_in_transaction()
	{
		return $this->in_transaction;
	}

	public function transaction_start()
	{
		if($this->in_transaction) {
			ErrorHandler::message(null, $this->dbname . " already in the middle of transaction, can't start new one", E_ERROR);
			return false;
		} else {
			$result = $this->query("START TRANSACTION", 'start transaction');
			if($result->is_error()) {
				return false;
			} else {
				$this->in_transaction = true;
				return true;
			}
		}
		break;
	}

	public function transaction_commit()
	{
		if(!$this->in_transaction) {
			ErrorHandler::message(null, $this->dbname . " not in a transaction, can't commit", E_ERROR);
			return false;
		} else {
			$result = $this->query("COMMIT", 'commit');
			if($result->is_error()) {
				return false;
			} else {
				$this->in_transaction = false;
				return true;
			}
		}
	}

	public function transaction_rollback()
	{
		if(!$this->in_transaction) {
			ErrorHandler::message(null, $this->dbname . " not in a transaction, can't rollback", E_ERROR);
			return false;
		} else {
			$result = $this->query("ROLLBACK", 'rollback');
			if($result->is_error()) {
				return false;
			} else {
				$this->in_transaction = false;
				return true;
			}
		}
	}

}
