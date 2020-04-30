<?php

/**
 * @ingroup SpideyDatabase
 * Returned by DbConnectionMySQL->query().
 */

class DbResultMySQL extends DbResult {
	
	protected $mysqli_result = false;
	protected $last_insert_id = false;
	
	public function __construct($db, $mysqli_result, $num_rows, $affected_rows)
	{
		$this->db = $db;
		$this->mysqli_result = $mysqli_result;
		$this->num_rows = $num_rows;
		$this->affected_rows = $affected_rows;
	}
	
	public function __destruct()
	{
		$this->free();
	}
	
	public function is_error()
	{
		return $this->mysqli_result === false;
	}
	
	public function get_error()
	{
		return $this->db->get_error();
	}
	
	public function get_errno()
	{
		return $this->db->get_errno();
	}
	
	public function num_rows()
	{
		return $this->num_rows;
	}
	
	public function affected_rows()
	{
		return $this->affected_rows;
	}

	public function set_last_insert_id($id)
	{
		$this->last_insert_id = $id;
	}
	
	public function insert_id()
	{
		return !empty($this->last_insert_id) ? $this->last_insert_id : $this->db->get_insert_id();
	}
	
	public function fetch_row()
	{
		$begin_time = microtime(true);
		$row = mysqli_fetch_assoc($this->mysqli_result);
		$end_time = microtime(true);
		
		$this->db->add_database_time($end_time-$begin_time);
		return $row;
	}
	
	public function seek($offset)
	{
		$ret = mysqli_data_seek($this->mysqli_result, $offset);
		
		if(!$ret) {
			ErrorHandler::message($result, 'mysqli_data_seek() failed', E_ERROR);
		}
		
		return $ret;
	}
	
	public function free()
	{
		if(!is_bool($this->mysqli_result)) {
			mysqli_free_result($this->mysqli_result);
		}
		
		$this->mysqli_result = false;
	}

}
